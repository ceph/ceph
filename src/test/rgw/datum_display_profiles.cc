// Built-in diagnostics and complete declaration enrollment share native/temporary semantics.
#include <datum/render/datum.h>
#include <datum/render/record.h>
#include <datum/compat/detail/datum_render.h>

#include <concepts>
#include <cstddef>
#include <type_traits>

#include <array>
#include <tuple>
#include <utility>

#include <functional>
#include <iterator>
#include <string>
#include <string_view>

#include <catch2/catch_test_macros.hpp>

namespace datum = ::ceph::salad::datum;
namespace iv = datum::interval;
namespace dr = datum::render;
namespace cd = datum::compat::detail;
using integer_domain = iv::comparison_domain<int>;
using integer_query = iv::query<integer_domain>;

namespace profile_test {

// No convenience predicates or duplicate point-type alias are required.
struct minimal_boundary final
{
    using domain_type = integer_domain;
    iv::boundary_ref<integer_domain> value;

    constexpr auto kind() const { return value.kind(); }
    constexpr const int& key() const { return value.key(); }
    constexpr bool inclusive() const { return value.inclusive(); }
};

struct opaque
{
    explicit opaque(int) {}
    opaque() = delete;
    opaque(const opaque&) = delete;
    opaque(opaque&&) = delete;
};
#define PRIVATE_FIELDS(Field) \
    Field(std::string_view, visible) \
    Field(std::string_view, secret) \
    Field(opaque, token)
DATUM_RECORD(private_record, PRIVATE_FIELDS);
#undef PRIVATE_FIELDS

#define REFERENCE_FIELDS(Field) Field(int&, observed) Field(const unsigned, revision)
DATUM_RECORD(reference_record, REFERENCE_FIELDS);
#undef REFERENCE_FIELDS

#define EMPTY_FIELDS(Field)
DATUM_RECORD(empty_record, EMPTY_FIELDS);
#undef EMPTY_FIELDS

struct point
{
    int value;
    static inline int copies = 0;
    static inline int moves = 0;

    explicit point(const int input) : value(input)
    {}
    point(const point& other) : value(other.value) { ++copies; }
    point(point&& other) noexcept : value(other.value) { ++moves; }

    point& operator=(const point&) = default;
    point& operator=(point&&) = default;
    auto operator<=>(const point&) const = default;
};
struct immovable
{
    int value;

    explicit immovable(const int input) : value(input)
    {}
    immovable() = delete;
    immovable(const immovable&) = delete;
    immovable(immovable&&) = delete;
};

} // profile_test

namespace ceph::salad::datum::render {

template <>
struct display_schema<profile_test::private_record>
#if defined(__cpp_lib_reflection)
    : reflected_display_schema<^^profile_test::private_record, 3,
                               ^^profile_test::private_record::secret,
                               ^^profile_test::private_record::token>
#else
    : declared_display_schema<profile_test::private_record, 3,
                                     &profile_test::private_record::secret,
                                     &profile_test::private_record::token>
#endif
{
    static constexpr std::string_view name = "private_record";
};

template <>
struct display_schema<profile_test::reference_record>
#if defined(__cpp_lib_reflection)
    : reflected_display_schema<^^profile_test::reference_record, 2>
#else
    : declared_display_schema<profile_test::reference_record, 2>
#endif
{
    static constexpr std::string_view name = "reference_record";
};

template <>
struct display_schema<profile_test::empty_record>
#if defined(__cpp_lib_reflection)
    : reflected_display_schema<^^profile_test::empty_record, 0>
#else
    : declared_display_schema<profile_test::empty_record, 0>
#endif
{
    static constexpr std::string_view name = "empty_record";
};

template <>
struct display_schema<profile_test::point>
{
    static constexpr std::string_view name = "point";
    static constexpr auto fields = std::tuple {field<&profile_test::point::value>("value")};
};

template <>
struct display_schema<profile_test::immovable>
{
    static constexpr std::string_view name = "immovable";
    static constexpr auto fields = std::tuple {field<&profile_test::immovable::value>("value")};
};

} // ceph::salad::datum::render

namespace {

template <typename T>
std::string text(const T& value, const dr::display_limits limits = {})
{
    std::string output;
    dr::format_to(std::back_inserter(output), value, limits);

    return output;
}

constexpr bool constant_profiles()
{
    const auto interval = integer_query::closed_open(1, 9);
    std::array<char, 256> output {};
    const auto last = dr::format_to(output.begin(), interval);

    return std::string_view(output.data(), last) ==
        "interval{lower=interval_boundary{kind=finite, key=1, inclusive=true}, "
        "upper=interval_boundary{kind=finite, key=9, inclusive=false}, empty=false}";
}

static_assert(constant_profiles());
using private_schema = datum::render::declared_display_schema<profile_test::private_record, 3,
    &profile_test::private_record::secret, &profile_test::private_record::token>;
static_assert(std::tuple_size_v<decltype(private_schema::fields)> == 3);
static_assert(std::get<1>(private_schema::fields).name == "secret");
static_assert(!std::default_initializable<profile_test::private_record>);
static_assert(!std::move_constructible<profile_test::private_record>);
using reference_schema = datum::render::declared_display_schema<profile_test::reference_record, 2>;
static_assert(std::tuple_size_v<decltype(reference_schema::fields)> == 2);

#if defined(__cpp_lib_reflection)
template <typename RecordT, std::meta::info Member, auto Accessor, dr::display_field_policy Policy>
consteval bool native_field_matches(const dr::display_field<Accessor, Policy> field)
{
    using expected_type = decltype((std::declval<const RecordT&>().[:Member:]));
    using actual_type = std::invoke_result_t<decltype(Accessor), const RecordT&>;

    return field.name == std::meta::identifier_of(Member) and
           std::same_as<expected_type, actual_type>;
}

template <typename RecordT>
consteval bool native_profile_matches()
{
    using portable = cd::datum_display_schema<RecordT>;
    using native = dr::display_schema<RecordT>;
    static constexpr auto members = datum::detail::record_members<RecordT>;
    static_assert(std::tuple_size_v<decltype(portable::fields)> == std::size(members));
    static_assert(portable::name == native::name);

    return []<std::size_t ...Index>(std::index_sequence<Index...>) {
        return (native_field_matches<RecordT, members[Index]>(
                    std::get<Index>(portable::fields)) and ...);
    }(std::make_index_sequence<std::size(members)> {});
}

static_assert(native_profile_matches<datum::render_detail::interval_boundary_display<
                  iv::boundary<integer_domain>>>());
static_assert(native_profile_matches<iv::lower_endpoint<integer_domain>>());
static_assert(native_profile_matches<iv::upper_endpoint<integer_domain>>());
static_assert(native_profile_matches<datum::traversal::page_traversal_result<int>>());
static_assert(native_profile_matches<datum::traversal::page_traversal_failure<int, int>>());
static_assert(native_profile_matches<datum::traversal::page_traversal_failure<
                  const profile_test::immovable&, const profile_test::immovable&>>());
static_assert(dr::enum_schema<iv::boundary_kind>::identifiers ==
              cd::datum_enum_schema<iv::boundary_kind>::identifiers);
static_assert(dr::enum_schema<iv::endpoint_inclusion>::identifiers ==
              cd::datum_enum_schema<iv::endpoint_inclusion>::identifiers);
static_assert(dr::enum_schema<datum::traversal::page_action>::identifiers ==
              cd::datum_enum_schema<datum::traversal::page_action>::identifiers);
static_assert(dr::enum_schema<datum::traversal::page_traversal_outcome>::identifiers ==
              cd::datum_enum_schema<datum::traversal::page_traversal_outcome>::identifiers);
#endif

} // anonymous namespace

TEST_CASE("built-in interval diagnostics preserve constexpr and infinity meaning", "[datum][render]")
{
    CHECK(constant_profiles());
    CHECK(text(integer_query::universal()) ==
          "interval{lower=interval_boundary{kind=negative_infinity, key=null, inclusive=false}, "
          "upper=interval_boundary{kind=positive_infinity, key=null, inclusive=false}, empty=false}");
    CHECK(text(iv::lower_at_or_after<integer_domain>(4)) ==
          "lower_endpoint{value=4, inclusion=included}");
    CHECK(text(iv::upper_before<integer_domain>(8)) ==
          "upper_endpoint{value=8, inclusion=excluded}");
    CHECK(text(integer_query::closed_open(7, 7)).ends_with("empty=true}"));
}

TEST_CASE("interval expression diagnostics do not compile or flatten their source", "[datum][render]")
{
    const auto selection = iv::without(integer_query::closed_open(1, 9), 5);
    CHECK(text(selection).starts_with("interval_difference{lhs=interval{"));
    const auto combined = iv::set_union(integer_query::closed_open(1, 3),
                                       integer_query::closed_open(5, 9));
    const auto overlap = iv::intersection(combined, integer_query::closed_open(2, 7));
    CHECK(text(combined).starts_with("interval_union{lhs=interval{"));
    CHECK(text(overlap).starts_with("interval_intersection{lhs=interval_union{"));
    const auto compiled = iv::compile_intervals<2>(selection);
    CHECK(text(compiled).starts_with("interval_set{intervals=[interval{"));
    std::string prefix;
    const auto result = dr::format_bounded(std::back_inserter(prefix), selection, {.bytes = 19});
    CHECK(result.truncated);
    CHECK(result.written == 19);
    CHECK(prefix == text(selection).substr(0, 19));
}

TEST_CASE("query diagnostics count policies without executing application operations", "[datum][render]")
{
    int called = 0;
    const auto query = datum::plan::with_control(
        datum::plan::with_residual(
            datum::plan::with_requirement(datum::plan::make_query(integer_query::singleton(3)),
                                          [&called] { ++called; }),
            [&called] { ++called; }), [&called] { ++called; });
    CHECK(text(query).ends_with("requirements=1, residuals=1, controls=1}"));
    CHECK(called == 0);
}

TEST_CASE("page diagnostics retain progress failure and cancellation semantics", "[datum][render]")
{
    using namespace datum::traversal;
    const page_traversal_result<int> progress {.resume = 8, .pages = 3,
        .outcome = page_traversal_outcome::stopped};
    CHECK(text(progress) == "page_traversal_result{resume=8, pages=3, outcome=stopped}");
    CHECK(text(page_traversal_result<int> {}) ==
          "page_traversal_result{resume=null, pages=0, outcome=complete}");
    CHECK(text(page_traversal_failure<int, std::string_view> {8, "failed", 3}) ==
          "page_traversal_failure{resume=8, error=\"failed\", pages=3}");
    datum::compat::stop_source source;
    const page_traversal_options options {source.get_token(), 2};
    CHECK(text(options) == "page_traversal_options{cancellation_requested=false, page_limit=2}");
    source.request_stop();
    CHECK(text(options) == "page_traversal_options{cancellation_requested=true, page_limit=2}");
}

TEST_CASE("built-in enum tables retain names and unknown numeric values", "[datum][render]")
{
    CHECK(text(iv::boundary_kind::finite) == "finite");
    CHECK(text(static_cast<iv::boundary_kind>(9)) == "unknown(9)");
    CHECK(text(iv::endpoint_inclusion::included) == "included");
    CHECK(text(static_cast<iv::endpoint_inclusion>(8)) == "unknown(8)");
    CHECK(text(datum::traversal::page_action::next) == "next");
    CHECK(text(static_cast<datum::traversal::page_action>(-1)) == "unknown(-1)");
    CHECK(text(datum::traversal::page_traversal_outcome::cancelled) == "cancelled");
    CHECK(text(datum::traversal::page_traversal_outcome::limit_reached) == "limit_reached");
}

TEST_CASE("checked declared diagnostics borrow unconstructible confidential records", "[datum][render]")
{
    const profile_test::private_record value {"public", "secret", profile_test::opaque {1}};
    CHECK(text(value) == "private_record{visible=\"public\", secret=<redacted:6 bytes>, token=<redacted>}");
    CHECK(text(value, {.bytes = 0}).empty());
    CHECK(text(profile_test::empty_record {}) == "empty_record{}");
    int observed = 7;
    const profile_test::reference_record reference {observed, 9};
    CHECK(text(reference) == "reference_record{observed=7, revision=9}");
    observed = 11;
    CHECK(text(reference) == "reference_record{observed=11, revision=9}");
}

TEST_CASE("profile rendering borrows nontrivial points and immovable progress", "[datum][render]")
{
    using point_domain = iv::comparison_domain<profile_test::point>;
    const auto query = iv::query<point_domain>::closed(
        profile_test::point {1}, profile_test::point {5});
    profile_test::point::copies = 0;
    profile_test::point::moves = 0;
    CHECK(text(query).find("key=point{value=1}") != std::string::npos);
    CHECK(profile_test::point::copies == 0);
    CHECK(profile_test::point::moves == 0);
    const profile_test::immovable state {13};
    const datum::traversal::page_traversal_failure<const profile_test::immovable&,
        const profile_test::immovable&> failure {state, state, 4};
    CHECK(text(failure) == "page_traversal_failure{resume=immovable{value=13}, "
          "error=immovable{value=13}, pages=4}");
}

TEST_CASE("local declaration diagnostics inspect types without constructing metadata witnesses",
          "[datum][render]")
{
#define LOCAL_FIELDS(Field) Field(int, value) Field(profile_test::opaque, secret)
    DATUM_LOCAL_RECORD(local, LOCAL_FIELDS);
#undef LOCAL_FIELDS
    using schema = datum::render::declared_display_schema<local, 2, &local::secret>;
    CHECK(std::get<0>(schema::fields).name == "value");
    CHECK(std::get<1>(schema::fields).name == "secret");
    const local source {7, profile_test::opaque {1}};
    std::string output;
    datum::render_detail::append_field_value(std::back_inserter(output), source, std::get<1>(schema::fields));
    CHECK(output == "<redacted>");
}

TEST_CASE("boundary diagnostics need only the public minimum protocol", "[datum][render]")
{
    const int coordinate = 4;
    const profile_test::minimal_boundary finite {iv::boundary_ref<integer_domain>::closed(coordinate)};
    STATIC_REQUIRE(iv::boundary_view<profile_test::minimal_boundary>);
    STATIC_REQUIRE(dr::displayable<profile_test::minimal_boundary>);
    CHECK(text(finite) == "interval_boundary{kind=finite, key=4, inclusive=true}");
    const profile_test::minimal_boundary infinity {iv::boundary_ref<integer_domain>::positive_infinity()};
    CHECK(text(infinity) == "interval_boundary{kind=positive_infinity, key=null, inclusive=false}");
    CHECK(text(integer_query::empty()) ==
          "interval{lower=interval_boundary{kind=positive_infinity, key=null, inclusive=false}, "
          "upper=interval_boundary{kind=negative_infinity, key=null, inclusive=false}, empty=true}");
}
