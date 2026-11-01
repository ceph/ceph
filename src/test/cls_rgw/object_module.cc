// RGW CLS: Actual object handlers with a controlled attribute/object boundary.
#include "test/cls_rgw/wire_test.h"
#include "module_loader.h"
#include "cls/rgw/cls_rgw_ops.h"

#include <catch2/catch_test_macros.hpp>

#include <cerrno>
#include <cstdint>
#include <cstdlib>

#include <fstream>
#include <map>
#include <string>
#include <utility>
#include <vector>

namespace {

using bytes = ::ceph::buffer::list;

using ::rgw_cls_test::canonical;

constexpr std::uint64_t pg_version = 0xFEDCBA9876543210ull;

bytes text(const std::string_view value)
{
    bytes output;
    output.append(value.data(), value.size());

    return output;
}

struct storage final
{
    std::map<std::string, bytes> attrs {{"keep.a", text("a")}, {"keep.b", text("b")}, {"other", text("c")}};
    bytes data = text("object data");
    ::ceph::real_time mtime {std::chrono::seconds {100} + std::chrono::milliseconds {500}};
    bool exists = true;
    std::vector<std::string> calls;
    bytes events;
    unsigned fail_at = 0;
    int error = -EIO;

public:
    int step(const std::string& name)
    {
        calls.push_back(name);
        ::ceph::encode(name, events);

        return fail_at == calls.size() ? error : 0;
    }

    bytes snapshot() const
    {
        bytes output;
        ::ceph::encode(attrs, output);
        ::ceph::encode(data, output);
        ::ceph::encode(mtime, output);
        ::ceph::encode(exists, output);

        return output;
    }

};

auto invoke(storage& state, const std::string& name, bytes input)
{
    const auto [call, flags] = ::rgw_cls_test::get_method(name);
    const int expected_flags = name == "obj_remove" ? CLS_METHOD_RD | CLS_METHOD_WR
                             : name == "obj_store_pg_ver" ? CLS_METHOD_WR : CLS_METHOD_RD;
    REQUIRE(flags == expected_flags);
    const auto before = state.snapshot();
    bytes output = text("prefix");
    const int status = call(&state, &input, &output);
    CHECK(output.to_str() == "prefix");
    if (const char *path = std::getenv("CEPH_TEST_CLS_RGW_TRANSCRIPT")) {
        static std::ofstream transcript {path, std::ios::binary};
        bytes record;
        ::ceph::encode(name, record);
        ::ceph::encode(input, record);
        ::ceph::encode(before, record);
        ::ceph::encode(status, record);
        ::ceph::encode(output, record);
        ::ceph::encode(state.calls, record);
        ::ceph::encode(state.events, record);
        ::ceph::encode(state.snapshot(), record);
        record.write_stream(transcript);
        REQUIRE(transcript.good());
    }

    return status;
}

bytes removal(const std::list<std::string>& prefixes)
{
    ::rgw_cls_obj_remove_op value;
    value.keep_attr_prefixes = prefixes;

    return canonical(value);
}

bytes prefix(const std::string& name, const bool fail)
{
    ::rgw_cls_obj_check_attrs_prefix value;
    value.check_prefix = name;
    value.fail_if_exist = fail;

    return canonical(value);
}

bytes mtime(const ::ceph::real_time time, const std::uint8_t kind,
            const bool precise, const std::uint8_t version = 2)
{
    bytes output;
    ENCODE_START(version, 1, output);
    ::ceph::encode(time, output);
    ::ceph::encode(kind, output);
    if (version >= 2) { ::ceph::encode(precise, output); }
    ENCODE_FINISH(output);

    return output;
}

} // anonymous namespace

int cls_cxx_getxattrs(cls_method_context_t context, std::map<std::string, bytes> *attrs)
{
    auto& state = *static_cast<storage *>(context);
    if (const int result = state.step("getxattrs"); result < 0) { return result; }
    if (!state.exists) { return -ENOENT; }
    *attrs = state.attrs;

    return 0;
}

int cls_cxx_remove(cls_method_context_t context)
{
    auto& state = *static_cast<storage *>(context);
    if (const int result = state.step("remove"); result < 0) { return result; }
    if (!state.exists) { return -ENOENT; }
    state.attrs.clear();
    state.data.clear();
    state.exists = false;

    return 0;
}

int cls_cxx_create(cls_method_context_t context, const bool exclusive)
{
    auto& state = *static_cast<storage *>(context);
    ::ceph::encode(exclusive, state.events);
    if (const int result = state.step("create"); result < 0) { return result; }
    CHECK_FALSE(exclusive);
    state.exists = true;

    return 0;
}

int cls_cxx_setxattr(cls_method_context_t context, const char *name, bytes *value)
{
    auto& state = *static_cast<storage *>(context);
    ::ceph::encode(std::string {name}, state.events);
    ::ceph::encode(*value, state.events);
    if (const int result = state.step("setxattr:" + std::string {name}); result < 0) { return result; }
    state.exists = true;
    state.attrs.insert_or_assign(name, *value);

    return 0;
}

std::uint64_t cls_current_version(cls_method_context_t context)
{
    auto& state = *static_cast<storage *>(context);
    state.step("current-version");

    return pg_version;
}

int cls_cxx_stat2(cls_method_context_t context, std::uint64_t *size, ::ceph::real_time *time)
{
    auto& state = *static_cast<storage *>(context);
    CHECK(size == nullptr);
    if (const int result = state.step("stat2"); result < 0) { return result; }
    if (!state.exists) { return -ENOENT; }
    *time = state.mtime;

    return 0;
}

TEST_CASE("Object removal preserves ordered prefix selection and native recreation effects", "[rgw][cls][object]")
{
    const std::vector<std::list<std::string>> cases {{}, {"absent"}, {"keep."}, {"keep.a", "keep.", "keep.a"}, {""}, {std::string {"keep.\0", 6}}};
    for (const auto& prefixes : cases) {
        storage state;
        const auto original = state.attrs;
        const int result = invoke(state, "obj_remove", removal(prefixes));
        CHECK(result == 0);
        std::map<std::string, bytes> expected;
        for (const auto& [name, value] : original) {
            for (const auto& prefix : prefixes) {
                if (name.starts_with(prefix)) { expected[name] = value; }
            }
        }
        CHECK(canonical(state.attrs).contents_equal(canonical(expected)));
        CHECK(state.exists == !expected.empty());
        CHECK(state.data.length() == 0);
        if (prefixes.empty()) {
            CHECK(state.calls == std::vector<std::string> {"remove"});
        } else {
            std::vector<std::string> calls {"getxattrs", "remove"};
            if (!expected.empty()) {
                calls.push_back("create");
                for (const auto& [name, value] : expected) { calls.push_back("setxattr:" + name); }
            }
            CHECK(state.calls == calls);
        }
    }
    storage state;
    state.attrs[std::string {"keep.a\0tail", 11}] = text("nul");
    CHECK(invoke(state, "obj_remove", removal({"keep."})) == 0);
    CHECK(state.attrs.size() == 2);
    CHECK(state.attrs.at("keep.a").to_str() == "nul");
}

TEST_CASE("Object removal preserves every native failure stage and missing-object result", "[rgw][cls][object]")
{
    for (unsigned stage = 1; stage <= 5; ++stage) {
        for (const int error : {-EIO, -EAGAIN, -ENOMEM}) {
            storage state;
            state.fail_at = stage;
            state.error = error;
            CHECK(invoke(state, "obj_remove", removal({"keep."})) == error);
            CHECK(state.calls.size() == stage);
        }
    }
    for (const auto& prefixes : std::vector<std::list<std::string>> {{}, {"keep."}, {""}}) {
        storage state;
        state.exists = false;
        state.attrs.clear();
        state.data.clear();
        CHECK(invoke(state, "obj_remove", removal(prefixes)) == -ENOENT);
        CHECK(state.calls.back() == "remove");
    }
    storage state;
    state.fail_at = 1;
    state.error = -ENOENT;
    CHECK(invoke(state, "obj_remove", removal({"keep."})) == 0);
    CHECK_FALSE(state.exists);
}

TEST_CASE("PG version storage retains native scalar bytes and attribute-name behavior", "[rgw][cls][object]")
{
    for (const auto& name : {std::string {}, std::string {"pg\0tail", 7}, std::string(80, 'v')}) {
        for (const int error : {0, -EIO, -EAGAIN}) {
            storage state;
            const auto before = state.snapshot();
            state.fail_at = 2;
            state.error = error;
            CHECK(invoke(state, "obj_store_pg_ver", canonical(::rgw_cls_obj_store_pg_ver_op {name})) == error);
            CHECK(state.calls == std::vector<std::string> {"current-version", "setxattr:" + std::string {name.c_str()}});
            if (error == 0) {
                CHECK(state.attrs.at(name.c_str()).contents_equal(canonical(pg_version)));
            } else {
                CHECK(state.snapshot().contents_equal(before));
            }
        }
    }
}

TEST_CASE("Attribute guards retain existence truth table and error ordering", "[rgw][cls][object]")
{
    for (const auto& name : {std::string {}, std::string {"keep."}, std::string {"keep.a"}, std::string {"missing"}, std::string {"keep.\0", 6}}) {
        for (const bool exists : {false, true}) {
            for (const bool fail : {false, true}) {
                storage state;
                state.exists = exists;
                const auto before = state.snapshot();
                const bool found = exists && (name == "keep." || name == "keep.a");
                const int expected = name.empty() ? -EINVAL : found == fail ? -ECANCELED : 0;
                CHECK(invoke(state, "obj_check_attrs_prefix", prefix(name, fail)) == expected);
                CHECK(state.calls.size() == (name.empty() ? 0 : 1));
                CHECK(state.snapshot().contents_equal(before));
            }
        }
    }
    for (const int error : {-EIO, -EAGAIN, -ENOMEM, -ENOENT}) {
        storage state;
        state.fail_at = 1;
        state.error = error;
        CHECK(invoke(state, "obj_check_attrs_prefix", prefix("keep.", true)) == (error == -ENOENT ? 0 : error));
    }
}

TEST_CASE("Mtime guards retain precision comparisons and missing-object epoch behavior", "[rgw][cls][object]")
{
    for (const bool exists : {false, true}) {
        for (const bool precise : {false, true}) {
            for (const std::uint8_t version : {1, 2}) {
                for (const auto request : {::ceph::real_time {}, ::ceph::real_time {std::chrono::seconds {100} + std::chrono::milliseconds {200}}, ::ceph::real_time {std::chrono::seconds {100} + std::chrono::milliseconds {800}}, ::ceph::real_time {std::chrono::seconds {101}}}) {
                    for (const std::uint8_t kind : {0, 1, 2, 3, 4, 5, 7, 255}) {
                        storage state;
                        state.exists = exists;
                        const auto before = state.snapshot();
                        auto left = exists ? state.mtime : ::ceph::real_time {};
                        auto right = request;
                        if (!precise || version < 2) {
                            left = ::ceph::real_clock::from_time_t(::ceph::real_clock::to_time_t(left));
                            right = ::ceph::real_clock::from_time_t(::ceph::real_clock::to_time_t(right));
                        }
                        const bool passed = kind == 0 ? left == right : kind == 1 ? left < right
                                          : kind == 2 ? left <= right : kind == 3 ? left > right : left >= right;
                        const int expected = kind > 4 ? -EINVAL : passed ? 0 : -ECANCELED;
                        CHECK(invoke(state, "obj_check_mtime", mtime(request, kind, precise, version)) == expected);
                        CHECK(state.calls == std::vector<std::string> {"stat2"});
                        CHECK(state.snapshot().contents_equal(before));
                    }
                }
            }
        }
    }
    for (const int error : {-EIO, -EAGAIN, -ENOMEM}) {
        storage state;
        state.fail_at = 1;
        state.error = error;
        CHECK(invoke(state, "obj_check_mtime", mtime({}, 255, true)) == error);
    }
}

TEST_CASE("Object handlers reject truncated or incompatible requests before storage", "[rgw][cls][object]")
{
    const std::map<std::string, bytes> requests {
        {"obj_remove", removal({"keep.", "other"})},
        {"obj_store_pg_ver", canonical(::rgw_cls_obj_store_pg_ver_op {"pg"})},
        {"obj_check_attrs_prefix", prefix("keep.", true)},
        {"obj_check_mtime", mtime({}, 0, true)}
    };
    for (const auto& [name, full] : requests) {
        for (unsigned size = 0; size < full.length(); ++size) {
            bytes input;
            input.substr_of(full, 0, size);
            storage state;
            const auto before = state.snapshot();
            CHECK(invoke(state, name, std::move(input)) == -EINVAL);
            CHECK(state.calls.empty());
            CHECK(state.snapshot().contents_equal(before));
        }
        bytes input;
        ENCODE_START(10, 20, input);
        ENCODE_FINISH(input);
        storage state;
        CHECK(invoke(state, name, std::move(input)) == -EINVAL);
        CHECK(state.calls.empty());
    }
}
