// The same built-in diagnostics work through each explicitly selected adapter.
#include <datum/render/datum.h>
#if defined(DATUM_TEST_STANDARD_FORMAT)
 #include <datum/render/std_format.h>
 namespace backend = std;
#else
 #include <datum/render/fmt.h>
 namespace backend = fmt;
#endif

#include <catch2/catch_test_macros.hpp>

namespace datum = ::ceph::salad::datum;
namespace iv = datum::interval;

TEST_CASE("formatting adapters preserve built-in Datum diagnostic policy", "[datum][render][profile]")
{
    const datum::traversal::page_traversal_result<int> progress {
        .resume = 8, .pages = 3, .outcome = datum::traversal::page_traversal_outcome::stopped};
    CHECK(backend::format("{}", progress) ==
          "page_traversal_result{resume=8, pages=3, outcome=stopped}");
    CHECK(backend::format("{}", datum::render::display(progress, {.bytes = 8})) == "page_tra");
    CHECK(backend::format("{}", iv::boundary_kind::finite) == "finite");
    CHECK(backend::format("{}", static_cast<iv::boundary_kind>(9)) == "unknown(9)");
    using domain = iv::comparison_domain<int>;
    const auto expression = iv::without(iv::query<domain>::closed_open(1, 9), 5);
    CHECK(backend::format("{}", expression).starts_with("interval_difference{lhs=interval{"));

    const auto bounded = iv::compile_intervals<2>(expression);
    const auto dynamic = iv::compile_intervals(expression);
    CHECK(backend::format("{}", datum::render::display(bounded)).starts_with(
        "interval_set{intervals=[interval{"));
    CHECK(backend::format("{}", datum::render::display(dynamic)).starts_with(
        "interval_set{intervals=[interval{"));
}
