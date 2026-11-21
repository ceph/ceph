// RGW CLS: Actual index-read builders and deferred completion contracts.
#include "test/cls_rgw/wire_test.h"
#include "cls/rgw/cls_rgw_client.h"
#include "cls/rgw/cls_rgw_const.h"
#include "maintenance_wire.h"

#include <catch2/catch_test_macros.hpp>

#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <new>
#include <utility>

#include <memory>
#include <string>
#include <string_view>

namespace {

using bytes = ceph::buffer::list;

using rgw_cls_test::canonical;

std::unique_ptr<librados::ObjectOperationCompletion> pending;
bytes request_bytes;
std::string request_method;
bool fail_allocation = false;
std::size_t allocation_countdown = std::numeric_limits<std::size_t>::max();

void capture_request(const char *cls, const char *method, const bytes& input)
{
    CHECK(std::string_view {cls} == "rgw");
    request_method = method;
    request_bytes = input;
}

// Keep the native encoder independent of the shared reply binding under test.
bytes canonical(const cls::rgw::bilog::reply<>& value)
{
    cls_rgw_bi_log_list_ret native;
    native.entries = value.entries;
    native.truncated = value.truncated;

    return canonical(native);
}

auto response()
{
    cls::rgw::bilog::reply<> value;
    value.entries.emplace_back();
    auto& entry = value.entries.back();
    entry.id = std::string {"id\0x", 4};
    entry.object = std::string(80, 'o');
    entry.tag = std::string(80, 't');
    entry.op = CLS_RGW_OP_DEL;
    entry.state = CLS_RGW_STATE_COMPLETE;
    entry.zones_trace.insert("zone", "location");
    value.truncated = true;

    return value;
}

void schedule(cls::rgw::bilog::reply<>& output, int& status)
{
    librados::ObjectReadOperation operation;
    std::string marker {"m\0r", 3};
    cls_rgw_bilog_list(operation, marker, 17, output, status);
    marker.assign(80, 'x');
}

void complete(const int status, bytes input, const bool allocation_failure = false)
{
    REQUIRE(pending != nullptr);
    auto callback = std::move(pending);
    fail_allocation = allocation_failure;
    callback->handle_completion(status, input);
    CHECK_FALSE(fail_allocation);
}

} // anonymous namespace

void *operator new(const std::size_t size)
{
    if (std::exchange(fail_allocation, false)) {
        throw std::bad_alloc {};
    }
    if (0 == allocation_countdown) {
        throw std::bad_alloc {};
    }
    if (allocation_countdown != std::numeric_limits<std::size_t>::max()) {
        --allocation_countdown;
    }
    if (void *p = std::malloc(size ? size : 1)) {
        return p;
    }

    throw std::bad_alloc {};
}

// Keep GCC from inlining free into callers of the matched malloc-backed replacements.
[[gnu::noinline]] void operator delete(void *p) noexcept { std::free(p); }
[[gnu::noinline]] void operator delete(void *p, std::size_t) noexcept { std::free(p); }

void librados::ObjectOperation::exec_impl(const char *cls, const char *method,
                                         bytes& input, ObjectOperationCompletion *completion)
{
    REQUIRE(pending == nullptr);
    capture_request(cls, method, input);
    pending.reset(completion);
}

void librados::ObjectOperation::exec_impl(const char *cls, const char *method, bytes& input)
{
    capture_request(cls, method, input);
}

void librados::ObjectOperation::exec_impl(const char *cls, const char *method,
                                         bytes& input, bytes *, int *)
{
    capture_request(cls, method, input);
}

TEST_CASE("Bucket list result preserves transport precedence and reports retry decode failures")
{
    struct statuses {
        int transport;
        int reply;
        int expected;
    };
    constexpr statuses cases[] {
            {0, 0, 0}, {0, 7, 7}, {7, 0, 0}, {7, 19, 19},
            {0, -EIO, -EIO}, {7, -ENOMEM, -ENOMEM},
            {0, RGWBIAdvanceAndRetryError, RGWBIAdvanceAndRetryError},
            {RGWBIAdvanceAndRetryError, RGWBIAdvanceAndRetryError, RGWBIAdvanceAndRetryError},
            {RGWBIAdvanceAndRetryError, -EIO, -EIO},
            {RGWBIAdvanceAndRetryError, -ENOMEM, -ENOMEM},
            {RGWBIAdvanceAndRetryError, 0, 0}, {RGWBIAdvanceAndRetryError, 7, 7},
            {-ETIMEDOUT, 0, -ETIMEDOUT}, {-ETIMEDOUT, -EIO, -ETIMEDOUT},
            {-ETIMEDOUT, RGWBIAdvanceAndRetryError, -ETIMEDOUT}, {-EIO, 7, -EIO}};
    for (const auto test : cases) {
        CAPTURE(test.transport, test.reply);
        CHECK(cls_rgw_bucket_list_result(test.transport, test.reply) == test.expected);
    }
}

TEST_CASE("Bilog client builders borrow markers and retain deferred result status", "[rgw][cls][bilog]")
{
    for (const int result : {0, 7, RGWBIAdvanceAndRetryError}) {
        cls::rgw::bilog::reply<> output;
        int status = 123;
        schedule(output, status);
        CHECK(status == 123);
        CHECK(request_method == "bi_log_list");
        cls_rgw_bi_log_list_op expected;
        expected.marker = std::string {"m\0r", 3};
        expected.max = 17;
        CHECK(request_bytes.contents_equal(canonical(expected)));
        const auto reply = response();
        complete(result, canonical(reply));
        CHECK(status == result);
        CHECK(canonical(output).contents_equal(canonical(reply)));
    }
    bytes expected;
    {
        cls_rgw_bi_log_trim_op native;
        native.start_marker = std::string {"s\0t", 3};
        native.end_marker = std::string(80, 'e');
        expected = canonical(native);
        librados::ObjectWriteOperation operation;
        cls_rgw_bilog_trim(operation, native.start_marker, native.end_marker);
        native.start_marker.clear();
        native.end_marker.clear();
    }
    CHECK(request_method == "bi_log_trim");
    CHECK(request_bytes.contents_equal(expected));
}

TEST_CASE("Bilog completion retains transport errors and contains malformed replies", "[rgw][cls][bilog]")
{
    auto output = response();
    const auto preserved = canonical(output);
    int status = 123;
    schedule(output, status);
    complete(-EAGAIN, {});
    CHECK(status == -EAGAIN);
    CHECK(canonical(output).contents_equal(preserved));

    const auto full = canonical(response());
    for (unsigned size = 0; size < full.length(); ++size) {
        output = response();
        bytes prefix;
        prefix.substr_of(full, 0, size);
        schedule(output, status);
        complete(0, prefix);
        CHECK(status == -EIO);
        CHECK(canonical(output).contents_equal(preserved));
    }
}

TEST_CASE("Bilog Datum completion preserves output on allocation failure", "[rgw][cls][datum][bilog]")
{
    auto output = response();
    const auto preserved = canonical(output);
    int status = 123;
    schedule(output, status);
    complete(0, canonical(response()), true);
    CHECK(status == -ENOMEM);
    CHECK(canonical(output).contents_equal(preserved));
}

namespace {

// Serialize through the independent native codec when checking public bucket pages.
bytes canonical(const cls::rgw::bucket_datum::reply& value)
{
    rgw_cls_list_ret native;
    native.dir.header = value.dir.header;
    native.dir.m = value.dir.m;
    native.marker = value.marker;
    native.is_truncated = value.is_truncated;
    native.cls_filtered = value.cls_filtered;

    return canonical(native);
}

auto bucket_response()
{
    cls::rgw::bucket_datum::reply value;
    value.dir.header.ver = 17;
    value.dir.header.max_marker = "max-marker";
    rgw_bucket_dir_entry entry;
    entry.key = {std::string {"n\0m", 3}, "instance"};
    entry.exists = true;
    entry.meta.etag = std::string(80, 'e');
    value.dir.m.emplace(std::string {"map\0key", 7}, std::move(entry));
    value.marker = {"marker", "version"};
    value.is_truncated = true;

    return value;
}

void schedule_bucket(cls::rgw::bucket_datum::reply& output, int& status)
{
    librados::ObjectReadOperation operation;
    cls_rgw_obj_key start {std::string {"n\0m", 3}, "instance"};
    std::string prefix(80, 'p');
    std::string delimiter {"/"};
    cls_rgw_bucket_list_op(operation, start, prefix, delimiter, 17, true, output, status);
    start.name.clear();
    prefix.clear();
    delimiter.clear();
}

} // anonymous namespace

TEST_CASE("Bucket list client owns borrowed requests and publishes complete page values", "[rgw][cls][bucket-list]")
{
    for (const int result : {0, 7, RGWBIAdvanceAndRetryError}) {
        cls::rgw::bucket_datum::reply output;
        int status = 123;
        schedule_bucket(output, status);
        CHECK(status == 123);
        CHECK(request_method == "bucket_list");
        rgw_cls_list_op expected;
        expected.start_obj = {std::string {"n\0m", 3}, "instance"};
        expected.filter_prefix = std::string(80, 'p');
        expected.delimiter = "/";
        expected.num_entries = 17;
        expected.list_versions = true;
        CHECK(request_bytes.contents_equal(canonical(expected)));
        auto reply = bucket_response();
        if (result == RGWBIAdvanceAndRetryError) {
            reply.dir.m.clear();
        }
        complete(result, canonical(reply));
        CHECK(status == result);
        CHECK(canonical(output).contents_equal(canonical(reply)));
        CHECK(output.cls_filtered);
    }
    for (const std::uint8_t version : {2, 3, 4}) {
        auto raw = canonical(bucket_response()).to_str();
        raw[0] = version;
        bytes input;
        input.append(raw);
        rgw_cls_list_ret expected;
        auto cursor = input.cbegin();
        ::decode(expected, cursor);
        auto output = bucket_response();
        output.cls_filtered = !expected.cls_filtered;
        output.dir.header.ver = 103;
        output.marker = {"preserved", "instance"};
        int status = 123;
        schedule_bucket(output, status);
        complete(0, input);
        CHECK(status == 0);
        CHECK(output.cls_filtered == expected.cls_filtered);
        CHECK(canonical(output).contents_equal(canonical(expected)));
    }
    auto output = bucket_response();
    const auto preserved = canonical(output);
    int status = 123;
    schedule_bucket(output, status);
    complete(-EAGAIN, {});
    CHECK(status == -EAGAIN);
    CHECK(canonical(output).contents_equal(preserved));
}

TEST_CASE("Bucket list completion reports decode and allocation failures", "[rgw][cls][bucket-list]")
{
    auto output = bucket_response();
    const auto preserved = canonical(output);
    const auto full = canonical(output);
    int status = 123;
    for (const int transport : {0, RGWBIAdvanceAndRetryError}) {
        for (unsigned size = 0; size < full.length(); ++size) {
            output = bucket_response();
            bytes prefix;
            prefix.substr_of(full, 0, size);
            schedule_bucket(output, status);
            complete(transport, prefix);
            CHECK(status == -EIO);
            CHECK(canonical(output).contents_equal(preserved));
        }
        output = bucket_response();
        schedule_bucket(output, status);
        complete(transport, full, true);
        CHECK(status == -ENOMEM);
        CHECK(canonical(output).contents_equal(preserved));
    }
}

TEST_CASE("Bucket retry completion rejects inconsistent pages and unusable progress",
          "[rgw][cls][bucket-list][continuation]")
{
    for (unsigned invalid = 0; invalid < 5; ++invalid) {
        auto output = bucket_response();
        const auto preserved = canonical(output);
        auto reply = bucket_response();
        reply.dir.m.clear();
        if (invalid == 0) {
            reply.marker = {std::string {"n\0m", 3}, "instance"};
        }
        if (invalid == 1) {
            reply.marker = {};
        }
        if (invalid == 2) {
            reply.is_truncated = false;
        }
        if (invalid == 3) {
            reply.dir.m = output.dir.m;
        }
        if (invalid == 4) {
            reply.marker = {"", "instance"};
        }
        int status = 123;
        schedule_bucket(output, status);
        complete(RGWBIAdvanceAndRetryError, canonical(reply));
        CHECK(status == -EIO);
        CHECK(cls_rgw_bucket_list_result(RGWBIAdvanceAndRetryError, status) == -EIO);
        CHECK(canonical(output).contents_equal(preserved));
    }
}

TEST_CASE("Bucket retry completion owns the submitted marker and compares both key fields",
          "[rgw][cls][bucket-list][continuation]")
{
    for (const auto& next : {
            cls_rgw_obj_key {std::string {"n\0m", 3}, "new-instance"},
            cls_rgw_obj_key {"a", "earlier-name"}}) {
        cls::rgw::bucket_datum::reply output;
        int status = 123;
        schedule_bucket(output, status);
        auto reply = bucket_response();
        reply.dir.m.clear();
        reply.marker = next;
        complete(RGWBIAdvanceAndRetryError, canonical(reply));
        CHECK(status == RGWBIAdvanceAndRetryError);
        CHECK(output.marker == next);
    }
    cls_rgw_obj_key start {std::string(80, 'n'), std::string(80, 'i')};
    cls::rgw::bucket_datum::reply output;
    int status = 123;
    librados::ObjectReadOperation operation;
    cls_rgw_bucket_list_op(operation, start, {}, {}, 1, true, output, status);
    start = {"next", std::string(80, 'j')};
    auto reply = bucket_response();
    reply.dir.m.clear();
    reply.marker = start;
    complete(RGWBIAdvanceAndRetryError, canonical(reply));
    CHECK(status == RGWBIAdvanceAndRetryError);
    CHECK(output.marker == start);
}

TEST_CASE("Ordinary bucket completions permit terminal pages without a continuation marker",
          "[rgw][cls][bucket-list][continuation]")
{
    for (const int result : {0, 7}) {
        for (const bool empty : {false, true}) {
            cls::rgw::bucket_datum::reply output;
            int status = 123;
            schedule_bucket(output, status);
            auto reply = bucket_response();
            reply.marker = {};
            reply.is_truncated = false;
            if (empty) {
                reply.dir.m.clear();
            }
            complete(result, canonical(reply));
            CHECK(status == result);
            CHECK(canonical(output).contents_equal(canonical(reply)));
        }
    }
}

TEST_CASE("Index stat and timeout clients encode borrowed native arguments", "[rgw][cls][stats]")
{
    for (const bool absolute : {false, true}) {
        for (const bool subtract : {false, true}) {
            bytes expected;
            {
                rgw_cls_bucket_update_stats_op reference;
                reference.absolute = absolute;
                reference.stats[RGWObjCategory::Main].total_size = 123;
                reference.stats[static_cast<RGWObjCategory>(255)].actual_size = 456;
                if (subtract) {
                    reference.dec_stats[RGWObjCategory::Main].num_entries = 3;
                }
                expected = canonical(reference);
                librados::ObjectWriteOperation operation;
                cls_rgw_bucket_update_stats(operation, absolute, reference.stats,
                    subtract ? &reference.dec_stats : nullptr);
                reference.stats.clear();
                reference.dec_stats.clear();
            }
            CHECK(request_method == "bucket_update_stats");
            CHECK(request_bytes.contents_equal(expected));
        }
    }
    for (const std::uint64_t timeout : {std::uint64_t {0}, std::uint64_t {120},
                                       std::numeric_limits<std::uint64_t>::max()}) {
        librados::ObjectWriteOperation operation;
        cls_rgw_bucket_set_tag_timeout(operation, timeout);
        CHECK(request_method == "bucket_set_tag_timeout");
        CHECK(request_bytes.contents_equal(canonical(rgw_cls_tag_timeout_op {.tag_timeout = timeout})));
    }
}

TEST_CASE("Index check client preserves synchronous decode and candidate error contracts", "[rgw][cls][stats]")
{
    rgw_cls_check_index_ret expected;
    expected.existing_header.ver = 17;
    expected.existing_header.stats[RGWObjCategory::Main].total_size = 23;
    expected.calculated_header.ver = 29;
    expected.calculated_header.stats[RGWObjCategory::MultiPart].actual_size = 31;
    const auto full = canonical(expected);
    rgw_cls_check_index_ret output;
    cls_rgw_bucket_check_index_decode(full, output);
    CHECK(canonical(output).contents_equal(full));
    for (unsigned size = 0; size < full.length(); ++size) {
        output = expected;
        bytes prefix;
        prefix.substr_of(full, 0, size);
        CHECK_THROWS_AS(cls_rgw_bucket_check_index_decode(prefix, output), ceph::buffer::error);
        CHECK(canonical(output).contents_equal(full));
    }
    output = expected;
    bool failed = false;
    fail_allocation = true;
    try
     {
        cls_rgw_bucket_check_index_decode(full, output);
     }
    catch (const std::bad_alloc&)
     {
        failed = true;
     }
    fail_allocation = false;
    CHECK(failed);
    CHECK(canonical(output).contents_equal(full));
}

TEST_CASE("Object client builders retain native wire bytes after borrowed inputs change", "[rgw][cls][object]")
{
    librados::ObjectWriteOperation operation;
    rgw_cls_obj_remove_op removal;
    removal.keep_attr_prefixes = {"", "z", std::string {"a\0b", 3}, "z", std::string(80, 'x')};
    auto expected = canonical(removal);
    cls_rgw_remove_obj(operation, removal.keep_attr_prefixes);
    removal.keep_attr_prefixes.clear();
    CHECK(request_method == "obj_remove");
    CHECK(request_bytes.contents_equal(expected));

    for (const auto& text : {std::string {}, std::string {"a\0b", 3}, std::string(80, 'x')}) {
        rgw_cls_obj_store_pg_ver_op version {text};
        expected = canonical(version);
        cls_rgw_obj_store_pg_ver(operation, version.attr);
        version.attr.clear();
        CHECK(request_method == "obj_store_pg_ver");
        CHECK(request_bytes.contents_equal(expected));
        for (const bool fail : {false, true}) {
            rgw_cls_obj_check_attrs_prefix prefix;
            prefix.check_prefix = text;
            prefix.fail_if_exist = fail;
            expected = canonical(prefix);
            cls_rgw_obj_check_attrs_prefix(operation, prefix.check_prefix, fail);
            prefix.check_prefix.clear();
            CHECK(request_method == "obj_check_attrs_prefix");
            CHECK(request_bytes.contents_equal(expected));
        }
    }
    for (unsigned kind = 0; kind < 8; ++kind) {
        for (const bool precise : {false, true}) {
            rgw_cls_obj_check_mtime check;
            check.mtime = ceph::real_time {std::chrono::seconds {123} + std::chrono::nanoseconds {987654321}};
            check.type = static_cast<RGWCheckMTimeType>(kind);
            check.high_precision_time = precise;
            expected = canonical(check);
            cls_rgw_obj_check_mtime(operation, check.mtime, precise, check.type);
            check.mtime = {};
            CHECK(request_method == "obj_check_mtime");
            CHECK(request_bytes.contents_equal(expected));
        }
    }
}

namespace {

namespace maintenance = rgw_cls_test::maintenance;

maintenance::page initial_check_page()
{
    maintenance::page page;
    page.cursor.version = 17;
    page.cursor.identity = std::string(80, 'i');
    page.cursor.marker = "a";
    page.existing_header.tag_timeout = 19;
    page.existing_header.ver = 5;
    page.existing_header.master_ver = 99;
    auto& stats = page.stats[RGWObjCategory::Main];
    stats.total_size = 7;
    stats.total_size_rounded = 4096;
    stats.num_entries = 1;
    stats.actual_size = 9;

    return page;
}

} // anonymous namespace

TEST_CASE("Maintenance client builders encode bounded native requests and own their inputs", "[rgw][cls][maintenance]")
{
    auto page = initial_check_page();
    auto cursor = page.cursor;
    auto expected = maintenance::check_request(cursor, 5, 3000);
    bytes output;
    librados::ObjectReadOperation read;
    cls_rgw_bucket_check_index(read, cursor, output, 5, 3000);
    cursor.identity.clear();
    cursor.marker.clear();
    CHECK(request_method == "bucket_check_index_page");
    CHECK(request_bytes.contents_equal(expected));

    rgw_cls_index_check check;
    check.cursor = page.cursor;
    check.cursor.phase = rgw_cls_index_phase::complete;
    check.cursor.marker.clear();
    check.result.calculated_header.stats = page.stats;
    expected = maintenance::rebuild_request(check.cursor, check.result.calculated_header.stats);
    librados::ObjectWriteOperation write;
    cls_rgw_bucket_rebuild_index(write, check);
    check = {};
    CHECK(request_method == "bucket_rebuild_index_commit");
    CHECK(request_bytes.contents_equal(expected));
}

TEST_CASE("Maintenance accumulation applies pages once and preserves native metadata policy", "[rgw][cls][maintenance]")
{
    auto page = initial_check_page();
    const auto first = page.encoded();
    rgw_cls_index_check check;
    REQUIRE(check.consume(first) == 0);
    CHECK(check.result.existing_header.master_ver == 99);
    CHECK(check.result.calculated_header.master_ver == 0);
    CHECK(check.result.calculated_header.tag_timeout == 19);
    CHECK(check.result.calculated_header.ver == 5);
    CHECK_FALSE(check.complete());
    const auto prior = canonical(check.result);
    CHECK(check.consume(first) == -EIO);
    CHECK(canonical(check.result).contents_equal(prior));

    page.cursor.phase = rgw_cls_index_phase::plain_high;
    page.cursor.marker.clear();
    REQUIRE(check.consume(page.encoded()) == 0);
    CHECK(check.result.calculated_header.stats.at(RGWObjCategory::Main).num_entries == 2);
    page.cursor.phase = rgw_cls_index_phase::instance;
    REQUIRE(check.consume(page.encoded()) == 0);
    page.cursor.phase = rgw_cls_index_phase::complete;
    REQUIRE(check.consume(page.encoded()) == 0);
    CHECK(check.complete());
    CHECK(check.result.calculated_header.stats.at(RGWObjCategory::Main).num_entries == 4);
    CHECK(check.consume(page.encoded()) == -EINVAL);
}

TEST_CASE("Malformed maintenance pages preserve cursor and previously accumulated results", "[rgw][cls][maintenance]")
{
    auto page = initial_check_page();
    rgw_cls_index_check seed;
    REQUIRE(seed.consume(page.encoded()) == 0);
    const auto saved = canonical(seed.result);
    page.cursor.phase = rgw_cls_index_phase::plain_high;
    page.cursor.marker.clear();
    const auto full = page.encoded();
    for (unsigned size = 0; size < full.length(); ++size) {
        bytes prefix;
        prefix.substr_of(full, 0, size);
        auto check = seed;
        CHECK(check.consume(prefix) == -EIO);
        CHECK(check.cursor == seed.cursor);
        CHECK(canonical(check.result).contents_equal(saved));
    }
    for (unsigned bad = 0; bad < 6; ++bad) {
        auto invalid = page;
        if (0 == bad) { ++invalid.cursor.version; }
        if (1 == bad) { invalid.cursor.identity = "other object"; }
        if (2 == bad) { invalid.cursor.phase = rgw_cls_index_phase::instance; }
        if (3 == bad) { invalid.cursor.marker = "unexpected marker"; }
        if (4 == bad) { invalid.cursor.version = 0; }
        if (5 == bad) { invalid.cursor.identity.clear(); }
        auto check = seed;
        CHECK(check.consume(invalid.encoded()) == -EIO);
        CHECK(check.cursor == seed.cursor);
        CHECK(canonical(check.result).contents_equal(saved));
    }
    auto trailing = full;
    trailing.append("x");
    CHECK(seed.consume(trailing) == -EIO);
    CHECK(canonical(seed.result).contents_equal(saved));
}

TEST_CASE("Every maintenance allocation failure leaves a page retryable exactly once", "[rgw][cls][maintenance]")
{
    auto page = initial_check_page();
    rgw_cls_index_check seed;
    REQUIRE(seed.consume(page.encoded()) == 0);
    const auto saved = canonical(seed.result);
    page.cursor.phase = rgw_cls_index_phase::plain_high;
    page.cursor.marker.clear();
    page.stats[static_cast<RGWObjCategory>(2)] = page.stats.at(RGWObjCategory::Main);
    const auto full = page.encoded();
    unsigned failures = 0;
    bool succeeded = false;
    for (unsigned budget = 0; budget < 64 && !succeeded; ++budget) {
        auto check = seed;
        bool failed = false;
        int result = -999;
        allocation_countdown = budget;
        try
         {
            result = check.consume(full);
         }
        catch (const std::bad_alloc&)
         {
            failed = true;
         }
        allocation_countdown = std::numeric_limits<std::size_t>::max();
        if (failed) {
            ++failures;
            CHECK(check.cursor == seed.cursor);
            CHECK(canonical(check.result).contents_equal(saved));
            REQUIRE(check.consume(full) == 0);
        }
        if (!failed) {
            REQUIRE(result == 0);
            succeeded = true;
        }
        CHECK(check.result.calculated_header.stats.at(RGWObjCategory::Main).num_entries == 2);
        CHECK(check.result.calculated_header.stats.at(static_cast<RGWObjCategory>(2)).num_entries == 1);
        CHECK(check.consume(full) == -EIO);
    }
    CHECK(failures > 0);
    CHECK(succeeded);
}
