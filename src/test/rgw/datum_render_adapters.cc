// Datum's explicit diagnostic arguments coexist with Ceph and range formatters.
#include "common/fmt_common.h"
#include <fmt/ranges.h>
#include <datum/render/fmt.h>

#include <concepts>
#include <iterator>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

#include <catch2/catch_test_macros.hpp>

namespace datum = ::ceph::salad::datum;
namespace dr = datum::render;

namespace render_adapter_test {

struct record final
{
    int shown;
    std::string_view secret;

public:
    record(int value, std::string_view hidden) : shown(value), secret(hidden)
    {}
    record(const record&) = delete;
    record(record&&) = delete;

    std::string fmt_print() const { return "legacy"; }
    const int *begin() const { return &shown; }
    const int *end() const { return &shown + 1; }

};

struct counted final
{
    int *calls;
    bool fail = false;
};

// Internal rendering calls must not find unrelated application helpers by ADL.
template <typename OutputT>
void format_value(OutputT, const counted&) = delete;

struct counted_sink final
{
    int *moves;

public:
    explicit counted_sink(int& count) : moves(&count)
    {}
    counted_sink(const counted_sink&) = delete;
    counted_sink(counted_sink&& source) noexcept : moves(source.moves) { ++*moves; }
    counted_sink& operator=(counted_sink&& source) noexcept
    {
        moves = source.moves;
        ++*moves;

        return *this;
    }

    counted_sink& operator*() noexcept { return *this; }
    counted_sink& operator++(int) noexcept { return *this; }
    void operator=(char) noexcept {}

};

class character_sink final
{
    std::string *target;

public:
    explicit character_sink(std::string& value) : target(&value)
    {}
    character_sink(character_sink&&) = default;
    character_sink& operator=(character_sink&&) = default;
    character_sink(const character_sink&) = delete;

    character_sink& operator*() noexcept { return *this; }
    character_sink& operator++(int) noexcept { return *this; }
    void operator=(const char value) { target->push_back(value); }

};

} // render_adapter_test

namespace ceph::salad::datum::render {

template <>
struct display_schema<render_adapter_test::record>
{
    static constexpr std::string_view name = "record";
    static constexpr auto fields = std::tuple {
        field<&render_adapter_test::record::shown>("shown"),
        display_field<&render_adapter_test::record::secret, display_field_policy::redacted> {
            "secret"}};
};

template <>
struct display_schema<render_adapter_test::counted>
{
    static constexpr std::string_view name = "counted";
    static constexpr auto fields = std::tuple {
        field<[](const auto& value) {
            ++*value.calls;
            if (value.fail) {
                throw std::runtime_error {"diagnostic read"};
            }
            return 1;
        }>("first"),
        field<[](const auto& value) { ++*value.calls; return 2; }>("second")};
};

} // ceph::salad::datum::render

namespace {

template <typename T>
concept borrows_temporary = requires(T&& value) { dr::display(std::move(value)); };

using diagnostic = dr::display_view<render_adapter_test::record>;
static_assert(std::same_as<
    decltype(dr::display_view {std::declval<const render_adapter_test::record&>()}), diagnostic>);
static_assert(std::same_as<
    decltype(dr::display(std::declval<const render_adapter_test::record&>())), diagnostic>);
// Constructor deduction drops const; an explicit template argument must retain its type.
static_assert(std::same_as<
    decltype(dr::display<const render_adapter_test::record>(
        std::declval<const render_adapter_test::record&>())), dr::display_view<const render_adapter_test::record>>);
static_assert(!borrows_temporary<render_adapter_test::record>);
static_assert(!borrows_temporary<const render_adapter_test::record>);
static_assert(!std::constructible_from<diagnostic, render_adapter_test::record&&>);
static_assert(!std::constructible_from<diagnostic, const render_adapter_test::record&&>);
static_assert(std::constructible_from<diagnostic, const render_adapter_test::record&>);
static_assert(std::is_trivially_copy_constructible_v<diagnostic>);
static_assert(!std::default_initializable<diagnostic>);

} // anonymous namespace

TEST_CASE("explicit diagnostics coexist with Ceph and range protocols", "[datum][render][adapter]")
{
    render_adapter_test::record source {7, "private"};
    const auto argument = dr::display(source);
    const auto copy = argument;
    CHECK(&argument.value() == &source);
    CHECK(&copy.value() == &source);
    CHECK(fmt::format("{}", argument) == "record{shown=7, secret=<redacted:7 bytes>}");
    source.shown = 9;
    CHECK(fmt::format("{}", copy) == "record{shown=9, secret=<redacted:7 bytes>}");
    fmt::memory_buffer buffer;
    fmt::format_to(std::back_inserter(buffer), "{}", argument);
    CHECK(std::string_view(buffer.data(), buffer.size()) ==
          "record{shown=9, secret=<redacted:7 bytes>}");
}

TEST_CASE("diagnostic arguments carry value limits and borrow observations", "[datum][render][limits]")
{
    int calls = 0;
    const render_adapter_test::counted source {&calls};
    auto limits = dr::display_limits {.bytes = 0};
    const auto argument = dr::display(source, limits);
    limits.bytes = 4096;
    CHECK(argument.limits().bytes == 0);
    CHECK(fmt::format("prefix{}suffix", argument) == "prefixsuffix");
    CHECK(calls == 0);
    std::string output;
    const auto result = dr::format_bounded(render_adapter_test::character_sink {output}, argument);
    CHECK(result.written == 0);
    CHECK(result.truncated);
    CHECK(calls == 0);
    const auto bounded = dr::display(source, {.bytes = 17});
    dr::format_to(render_adapter_test::character_sink {output}, bounded);
    CHECK(output == "counted{first=1, ");
    CHECK(calls == 1);
}

TEST_CASE("diagnostic arguments retain parse and reader failure boundaries", "[datum][render][errors]")
{
    int calls = 0;
    const render_adapter_test::counted source {&calls, true};
    const auto argument = dr::display(source);
    CHECK_THROWS_AS(fmt::vformat("{:>8}", fmt::make_format_args(argument)), fmt::format_error);
    CHECK(calls == 0);
    CHECK_THROWS_AS(fmt::format("{}", argument), std::runtime_error);
    CHECK(calls == 1);
    calls = 0;
    CHECK(fmt::format("{}", dr::display(source, {.bytes = 0})).empty());
    CHECK(calls == 0);
}

TEST_CASE("direct diagnostic formatting does not admit application ADL helpers", "[datum][render][adapter]")
{
    int calls = 0;
    const render_adapter_test::counted source {&calls};
    CHECK(fmt::format("{}", source) == "counted{first=1, second=2}");
    CHECK(calls == 2);
}

TEST_CASE("explicit diagnostic arguments add no sink moves", "[datum][render][adapter]")
{
    int calls = 0;
    const render_adapter_test::counted source {&calls};
    int direct = 0, wrapped = 0;
    dr::format_to(render_adapter_test::counted_sink {direct}, source);
    dr::format_to(render_adapter_test::counted_sink {wrapped}, dr::display(source));
    CHECK(wrapped == direct);
    direct = wrapped = 0;
    const auto first = dr::format_bounded(render_adapter_test::counted_sink {direct}, source);
    const auto second = dr::format_bounded(render_adapter_test::counted_sink {wrapped}, dr::display(source));
    CHECK(wrapped == direct);
    CHECK(first.written == second.written);
}
