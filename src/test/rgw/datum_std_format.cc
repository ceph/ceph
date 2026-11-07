// The standard adapter is optional and does not import fmt.
#include <datum/render/std_format.h>

#include <array>
#include <concepts>
#include <iterator>
#include <string>
#include <string_view>

#include <catch2/catch_test_macros.hpp>

namespace datum = ::ceph::salad::datum;
namespace dr = datum::render;

namespace standard_adapter_test {

struct record final
{
    int shown;
};

} // standard_adapter_test

namespace ceph::salad::datum::render {

template <>
struct display_schema<standard_adapter_test::record>
{
    static constexpr std::string_view name = "record";
    static constexpr auto fields = std::tuple {field<&standard_adapter_test::record::shown>("shown")};
};

} // ceph::salad::datum::render

static_assert(std::formattable<standard_adapter_test::record, char>);
static_assert(std::formattable<dr::display_view<standard_adapter_test::record>, char>);
static_assert([] {
    const standard_adapter_test::record source {7};
    std::array<char, 64> output {};
    const auto last = dr::format_to(output.begin(), dr::display(source));

    return std::string_view(output.data(), last) == "record{shown=7}";
}());

#if defined(__cpp_lib_constexpr_format) and __cpp_lib_constexpr_format >= 202511L
static_assert([] {
    const standard_adapter_test::record source {7};

    return std::format("{}", source) == std::format("{}", dr::display(source));
}());
#endif

TEST_CASE("standard diagnostics share the bounded engine and explicit argument",
          "[datum][render][std-format]")
{
    const standard_adapter_test::record source {7};
    CHECK(std::format("{}", source) == "record{shown=7}");
    CHECK(std::format("{}", dr::display(source)) == "record{shown=7}");
    CHECK(std::format("prefix{}suffix", dr::display(source, {.bytes = 0})) == "prefixsuffix");
    CHECK(std::format("{}", dr::display(source, {.bytes = 8})) == "record{s");
    const auto argument = dr::display(source);
    CHECK_THROWS_AS(std::vformat("{:>8}", std::make_format_args(argument)), std::format_error);
}
