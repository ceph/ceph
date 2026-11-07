// Explicit-schema diagnostics have the same bounded behavior in C++23 and native C++26.
#include <datum/render/display.h>
#include <datum/compat/charconv.h>
#include <datum/compat/detail/render.h>

#include <concepts>
#include <cstdint>
#include <limits>
#include <type_traits>

#include <optional>
#include <stdexcept>
#include <tuple>
#include <variant>

#include <array>
#include <map>
#include <vector>

#include <iterator>
#include <ranges>
#include <string>
#include <string_view>

#include <chrono>
#include <ratio>

#include <catch2/catch_test_macros.hpp>

namespace datum = ::ceph::salad::datum;
namespace dr = datum::render;

namespace render_test {

struct empty {};
struct empty_tuple { std::tuple<> values; };
struct range_record { const std::array<int, 24>& values; int *reads; };

struct record
{
    std::int64_t low;
    std::uint64_t high;
    std::string_view text;
};
struct vocabulary
{
    std::tuple<bool, char, int, double, std::chrono::milliseconds> values;
    std::optional<int> present;
    std::variant<std::monostate, int> choice;
    std::map<std::string, int> mapping;
    std::chrono::time_point<std::chrono::steady_clock, std::chrono::seconds> time;
};
struct counted
{
    std::vector<int> *order;
    bool fail;
};
struct confidential
{
    int *observed;
    std::string_view secret;
};
struct cycle { const cycle *next; };
struct node
{
    int value;
    std::vector<node> children;
};
struct immovable
{
    int value = 7;
    immovable() = default;
    immovable(const immovable&) = delete;
    immovable(immovable&&) = delete;
};
struct borrowed_tuple { const std::tuple<immovable>& values; };
struct guarded
{
    int value;

    using rep = int;
    using period = int;

    template <typename T = void>
    auto begin() const
    {
        static_assert(!std::same_as<T, void>, "Unselected range recognition must remain deferred");

        return &value;
    }

    auto end() const { return &value + 1; }

};
struct binary { std::array<std::byte, 40> bytes; };

struct guarded_text final
{
    using rep = int;
    using period = int;

    constexpr operator std::string_view() const { return "guarded text"; }
};

enum struct letter : unsigned char { first = 1, alias = 1 };
enum struct bit : bool { zero = false, one = true };
struct enumerations { letter character; bit boolean; };
struct elapsed : std::chrono::milliseconds { using std::chrono::milliseconds::milliseconds; };
struct units { elapsed milli; std::chrono::duration<int, std::ratio<2, 3>> ratio; };

} // render_test

namespace ceph::salad::datum::render {

template <> struct display_schema<render_test::empty>
{
    static constexpr std::string_view name = "empty";
    static constexpr auto fields = std::tuple {};
};
template <> struct display_schema<render_test::empty_tuple>
{
    static constexpr std::string_view name = "empty_tuple";
    static constexpr auto fields = std::tuple {field<&render_test::empty_tuple::values>("values")};
};
template <> struct display_schema<render_test::range_record>
{
    static constexpr std::string_view name = "range";
    static constexpr auto fields = std::tuple {field<[](const auto& value) {
        return value.values | std::views::transform([&value](const int item) {
            ++*value.reads;
            return item;
        });
    }>("values")};
};
template <> struct display_schema<render_test::record>
{
    static constexpr std::string_view name = "record";
    static constexpr auto fields = std::tuple {field<&render_test::record::low>("low"),
        field<&render_test::record::high>("high"), field<&render_test::record::text>("text")};
};
template <> struct display_schema<render_test::vocabulary>
{
    static constexpr std::string_view name = "vocabulary";
    static constexpr auto fields = std::tuple {field<&render_test::vocabulary::values>("values"),
        field<&render_test::vocabulary::present>("present"),
        field<&render_test::vocabulary::choice>("choice"),
        field<&render_test::vocabulary::mapping>("mapping"), field<&render_test::vocabulary::time>("time")};
};
template <> struct display_schema<render_test::counted>
{
    static constexpr std::string_view name = "counted";
    static constexpr auto fields = std::tuple {
        field<[](const auto& value) { value.order->push_back(1); return 1; }>("first"),
        field<[](const auto& value) {
            value.order->push_back(2);
            if (value.fail) { throw std::runtime_error {"diagnostic field"}; }
            return 2;
        }>("second"),
        field<[](const auto& value) { value.order->push_back(3); return 3; }>("third")};
};
template <> struct display_schema<render_test::confidential>
{
    static constexpr std::string_view name = "private";
    static constexpr auto fields = std::tuple {
        display_field<[](const auto& value) { ++*value.observed; return value.secret; },
                      display_field_policy::redacted> {"sized"},
        display_field<[](const auto& value) -> int {
            ++*value.observed; throw std::runtime_error {"redacted scalar"};
        }, display_field_policy::redacted> {"scalar"}};
};
template <> struct display_schema<render_test::node>
{
    static constexpr std::string_view name = "node";
    static constexpr auto fields = std::tuple {field<&render_test::node::value>("value"),
        field<&render_test::node::children>("children")};
};
template <> struct display_projection<render_test::cycle>
{
    static const auto& get(const render_test::cycle& value) { return *value.next; }
};
template <> struct display_projection<render_test::immovable>
{
    static constexpr int get(const render_test::immovable& value) { return value.value; }
};
template <> struct display_schema<render_test::borrowed_tuple>
{
    static constexpr std::string_view name = "borrowed";
    static constexpr auto fields = std::tuple {
        field<[](const auto& value) -> decltype(auto) { return (value.values); }>("values")};
};
template <> struct display_schema<render_test::guarded>
{
    static constexpr std::string_view name = "guarded";
    static constexpr auto fields = std::tuple {field<&render_test::guarded::value>("value")};
};
template <> struct display_projection<render_test::guarded>
{
    template <typename T = void>
    static auto get(const render_test::guarded&)
    {
        static_assert(!std::same_as<T, void>, "Record recognition must precede projection recognition");
        throw std::runtime_error {"unselected projection"};

        return 0;
    }

};
template <> struct display_schema<render_test::binary>
{
    static constexpr std::string_view name = "binary";
    static constexpr auto fields = std::tuple {field<&render_test::binary::bytes>("bytes")};
};
template <> struct enum_schema<render_test::letter>
{
    static constexpr std::string_view name(const render_test::letter value)
    {
        return value == render_test::letter::first ? "first" : "";
    }

};
template <> struct enum_schema<render_test::bit>
{
    static constexpr std::string_view name(const render_test::bit value)
    {
        return value == render_test::bit::zero ? "zero" : "";
    }

};
template <> struct display_schema<render_test::enumerations>
{
    static constexpr std::string_view name = "enums";
    static constexpr auto fields = std::tuple {field<&render_test::enumerations::character>("character"),
        field<&render_test::enumerations::boolean>("boolean")};
};
template <> struct display_schema<render_test::units>
{
    static constexpr std::string_view name = "units";
    static constexpr auto fields = std::tuple {field<&render_test::units::milli>("milli"),
        field<&render_test::units::ratio>("ratio")};
};

} // ceph::salad::datum::render

namespace {

template <typename T>
std::string text(const T& value, dr::display_limits limits = {})
{
    std::string output;
    dr::format_to(std::back_inserter(output), value, limits);
    return output;
}

constexpr bool render_constants()
{
    const render_test::record value {std::numeric_limits<std::int64_t>::min(),
        std::numeric_limits<std::uint64_t>::max(), std::string_view {"a\0b", 3}};
    std::array<char, 256> output {};
    const auto last = dr::format_to(output.begin(), value);
    return std::string_view(output.data(), last) ==
        "record{low=-9223372036854775808, high=18446744073709551615, text=\"a\\x00b\"}";
}

} // anonymous namespace

static_assert(render_constants());
static_assert(datum::render_detail::is_optional_v<const std::optional<int>&>);
static_assert(datum::render_detail::is_variant_v<volatile std::variant<int, bool>>);
static_assert(datum::render_detail::display_tuple<const std::tuple<int>&>);
static_assert(datum::render_detail::display_tuple<std::pair<int, bool>>);
static_assert(!datum::render_detail::display_tuple<std::array<int, 2>>);
static_assert(!datum::render_detail::is_optional_v<render_test::guarded>);
static_assert(datum::render_detail::duration_value<render_test::elapsed>);

TEST_CASE("explicit diagnostics preserve integer limits and escaped bytes", "[datum][render]")
{
    CHECK(render_constants());
    const render_test::record value {-7, 42, std::string_view {"\x80\n\"\\", 4}};
    CHECK(text(value) == "record{low=-7, high=42, text=\"\\x80\\n\\\"\\\\\"}");
}

TEST_CASE("exact standard vocabulary renders through explicit schemas", "[datum][render]")
{
    using clock_time = decltype(render_test::vocabulary::time);
    render_test::vocabulary value {{true, 'x', -2, 1.25, std::chrono::milliseconds {4}}, 5,
        std::monostate {}, {{"k", 8}}, clock_time {std::chrono::seconds {6}}};
    CHECK(text(value) == "vocabulary{values=(true, x, -2, 1.25, 4ms), present=5, "
          "choice=none, mapping=[(\"k\", 8)], time=6s}");
    value.present.reset();
    value.choice = 9;
    const auto changed = text(value);
    CHECK(changed.find("present=null, choice=9") != std::string::npos);
}

TEST_CASE("diagnostic field order and failure prefixes are stable", "[datum][render]")
{
    std::vector<int> order;
    const render_test::counted value {&order, true};
    std::string output;
    CHECK_THROWS_AS(dr::format_to(std::back_inserter(output), value), std::runtime_error);
    CHECK(order == std::vector<int> {1, 2});
    CHECK(output == "counted{first=1, second=");
    order.clear();
    const auto result = dr::format_bounded(std::back_inserter(output), value, {.bytes = 0});
    CHECK(result.truncated);
    CHECK(result.written == 0);
    CHECK(order.empty());
}

TEST_CASE("redacted sizes observe once and redacted scalars skip access", "[datum][render]")
{
    int observed = 0;
    const render_test::confidential value {&observed, "secret"};
    CHECK(text(value) == "private{sized=<redacted:6 bytes>, scalar=<redacted>}");
    CHECK(observed == 1);
    observed = 0;
    CHECK(text(value, {.bytes = 0}).empty());
    CHECK(observed == 0);
}

TEST_CASE("diagnostics preserve immovable tuple members and category precedence", "[datum][render]")
{
    const std::tuple<render_test::immovable> values {};
    const render_test::borrowed_tuple holder {values};
    CHECK(text(holder) == "borrowed{values=(7)}");
    CHECK(text(render_test::guarded {3}) == "guarded{value=3}");

    std::string output;
    datum::render_detail::append_value(std::back_inserter(output), render_test::guarded_text {});
    CHECK(output == "\"guarded text\"");
}

TEST_CASE("diagnostic depth and binary caps bound nested work", "[datum][render]")
{
    render_test::cycle value {};
    value.next = &value;
    std::string output;
    const auto result = dr::format_bounded(std::back_inserter(output), value, {.depth = 3});
    CHECK(result.truncated);
    CHECK(output.empty());
    render_test::binary bytes {};
    for (std::size_t index = 0; index < bytes.bytes.size(); ++index) {
        bytes.bytes[index] = static_cast<std::byte>(index);
    }
    CHECK(text(bytes) == "binary{bytes=hex\"000102030405060708090A0B0C0D0E0F"
          "101112131415161718191A1B1C1D1E1F...\"}");
}

TEST_CASE("explicit enum aliases and unknown numeric values preserve meaning", "[datum][render]")
{
    CHECK(text(render_test::enumerations {render_test::letter::alias, render_test::bit::zero}) ==
          "enums{character=first, boolean=zero}");
    CHECK(text(render_test::enumerations {static_cast<render_test::letter>(2), render_test::bit::one}) ==
          "enums{character=unknown(2), boolean=unknown(1)}");
    CHECK(text(render_test::units {render_test::elapsed {4}, std::chrono::duration<int, std::ratio<2, 3>> {3}}) ==
          "units{milli=4ms, ratio=3[2/3]s}");
}

TEST_CASE("constexpr decimal conversion agrees with standard integer output", "[datum][render][charconv]")
{
    auto check = []<typename T>() {
        for (const T value : std::array<T, 5> {std::numeric_limits<T>::min(),
                 std::numeric_limits<T>::max(), T {0}, T {1}, T {10}}) {
            std::array<char, 64> expected {}, actual {};
            const auto native = std::to_chars(expected.data(), expected.data() + expected.size(), value);
            const auto fallback = datum::compat::detail::decimal_chars(
                actual.data(), actual.data() + actual.size(), value);
            CHECK(native.ec == fallback.ec);
            CHECK(std::string_view(expected.data(), native.ptr) ==
                  std::string_view(actual.data(), fallback.ptr));
        }
    };
    check.template operator()<std::int8_t>();
    check.template operator()<std::uint8_t>();
    check.template operator()<std::int16_t>();
    check.template operator()<std::uint16_t>();
    check.template operator()<std::int32_t>();
    check.template operator()<std::uint32_t>();
    check.template operator()<std::int64_t>();
    check.template operator()<std::uint64_t>();
    CHECK([] {
        std::array<char, 1> out {};
        const auto result = datum::compat::to_decimal_chars(out.data(), out.data() + out.size(), -7);
        return result.ptr == out.data() + out.size() and result.ec == std::errc::value_too_large;
    }());
}

TEST_CASE("empty diagnostics and tuple traversal retain minimal requirements", "[datum][render]")
{
    CHECK(text(render_test::empty {}) == "empty{}");
    CHECK(text(render_test::empty_tuple {}) == "empty_tuple{values=()}");
    struct visitor {
        int calls = 0;
        visitor() = default;
        visitor(const visitor&) = delete;
        visitor(visitor&&) = delete;
        void operator()(int) & { ++calls; }
    } function;
    const std::tuple values {1, 2, 3};
    datum::compat::detail::visit_display_tuple(values, function);
    CHECK(function.calls == 3);
    datum::compat::detail::visit_display_tuple(std::tuple {}, function);
    CHECK(function.calls == 3);
}

TEST_CASE("range display caps stop element observations", "[datum][render]")
{
    std::array<int, 24> values {};
    for (std::size_t index = 0; index < values.size(); ++index) {
        values[index] = static_cast<int>(index);
    }
    int reads = 0;
    const render_test::range_record value {values, &reads};
    CHECK(text(value) == "range{values=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, ...]}");
    CHECK(reads == 16);
    reads = 0;
    CHECK(text(value, {.bytes = 0}).empty());
    CHECK(reads == 0);
}

TEST_CASE("recursive record diagnostics retain runtime depth bounds", "[datum][render]")
{
    const render_test::node value {1, {{2, {}}}};
    CHECK(text(value) == "node{value=1, children=[node{value=2, children=[]}]}");
    std::string output;
    const auto result = dr::format_bounded(std::back_inserter(output), value, {.depth = 2});
    CHECK(result.truncated);
    CHECK(output == "node{value=1, children=[]}");
    CHECK(result.written == output.size());
}
