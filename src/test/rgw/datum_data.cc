// Datum reflected records, ownership, diagnostics and optional codecs tests.

#include "common/container_concepts.h"
#include "datum_portable_support.h"
#include <catch2/catch_test_macros.hpp>
#include <datum/budget.h>
#include <datum/bytes.h>
#include <datum/interval/clip.h>
#include <datum/interval/metric.h>
#include <datum/interval/query.h>
#include <datum/interval/range.h>
#include <datum/key.h>
#include <datum/ownership.h>
#include <datum/plan/query.h>
#include <datum/record.h>
#include <datum/render/datum.h>
#include <datum/render/display.h>
#include <datum/render/fmt.h>
#include <datum/render/std_format.h>
#include <datum/traversal.h>
#include <datum/traversal/scan.h>

#include <compare>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <type_traits>

#include <expected>
#include <optional>
#include <stdexcept>
#include <system_error>
#include <utility>

#include <array>
#include <map>
#include <set>
#include <vector>

#include <memory>
#include <memory_resource>

#include <algorithm>
#include <functional>
#include <initializer_list>
#include <iterator>
#include <ranges>

#include <format>
#include <sstream>
#include <string>
#include <string_view>

#ifdef DATUM_HAVE_ZPP
 #include <datum/codec/zpp.h>

#endif

namespace datum = ::ceph::salad::datum;
namespace iv = datum::interval;

// Datum data mechanics: neutral fragmentation, retention and later data profiles.
// These fixtures keep all native storage, codecs and execution outside Datum.

struct mechanics_row
{
    std::string key;
    std::vector<unsigned char> payload;
};
struct mechanics_pair
{
    int first;

    int second;
};
using row_fields = datum::member_fields<&mechanics_row::key, &mechanics_row::payload>;
using reversed_fields = datum::member_fields<&mechanics_pair::second, &mechanics_pair::first>;

static_assert(reversed_fields::covers<mechanics_pair>());
static_assert(!datum::member_fields<&mechanics_pair::first,
                                    &mechanics_pair::first>::covers<mechanics_pair>());
template <auto Member>
concept valid_member_profile = requires { typename datum::member_fields<Member>; };
static_assert(!valid_member_profile<static_cast<int mechanics_pair::*>(nullptr)>);
static_assert([] {
    int sequence = 0;
    const auto value =
        reversed_fields::reconstruct<mechanics_pair>([&](auto) { return ++sequence; });

    return value.first == 2 and value.second == 1;
}());

template <typename T>
concept temporary_projection = requires(T value) { std::move(value).get(); };
static_assert(!temporary_projection<datum::retained_view<std::string, datum::byte_slice>>);

TEST_CASE("one field layout reconstructs and moves final owners")
{
    const auto row = row_fields::reconstruct<mechanics_row>([](auto field) {
        using value_type = typename decltype(field)::value_type;

        if constexpr (std::same_as<value_type, std::string>) {
            return std::string(128, 'k');
        }

        if constexpr (!std::same_as<value_type, std::string>) {
            return std::vector<unsigned char>(256, 7);
        }
    });
    CHECK(std::size(row.key) == 128);
    CHECK(std::size(row.payload) == 256);
    auto source = row;
    const auto *key_address = std::data(source.key);
    const auto *payload_address = std::data(source.payload);
    const auto target = row_fields::project<mechanics_row>(std::move(source));
    CHECK(std::data(target.key) == key_address);
    CHECK(std::data(target.payload) == payload_address);
    CHECK_THROWS_AS(row_fields::reconstruct<mechanics_row>(
                        [](auto field) -> typename decltype(field)::value_type {
                            throw std::runtime_error {"bad field"};
                        }),
                    std::runtime_error);
    const auto explicit_record = reversed_fields::construct(
        [](auto) { return 4; },
        [](int second, int first) { return mechanics_pair {first, second}; });
    CHECK(explicit_record.first == 4);
}

static_assert(datum::owning_value<std::map<std::string, std::vector<int>>>);
static_assert(datum::owning_value<std::tuple<int, std::unique_ptr<std::string>>>);
static_assert(datum::owning_value<std::expected<void, std::string>>);
static_assert(!datum::owning_value<std::tuple<int&>>);
static_assert(!datum::owning_value<std::pmr::vector<int>>);
static_assert(!datum::owning_value<std::set<int, std::greater<int>>>);
static_assert(!datum::owning_value<std::shared_ptr<std::string>>);
static_assert([] {
    datum::unlimited_budget budget;
    auto nesting = budget.enter_depth();
    budget.charge(datum::resource::work, 1);

    return true;
}());

class constructed_pair final
{
    int first;

    int second;

public:
    constructed_pair() = delete;
    constexpr constructed_pair(int left, int right) : first(left), second(right)
    {}

public:
    constexpr int sum() const { return first + second; }

};
template <>
struct ceph::salad::datum::ownership_traits<constructed_pair> : std::true_type
{};
static_assert([] {
    const auto value = reversed_fields::construct(
        [](auto) { return 3; },
        [](int second, int first) { return constructed_pair {first, second}; });

    return value.sum() == 6;
}());

// Datum constant-evaluation proofs: compose data facilities before runtime.
// Keep records and effects neutral; these checks need no codec or native backend.

namespace {

struct offsets final
{
    using value_type = unsigned;

public:
    static constexpr auto compare(const unsigned lhs, const unsigned rhs) { return lhs <=> rhs; }

};

struct row final
{
    unsigned first;
    unsigned last;

    std::array<unsigned char, 8> payload;
};

constexpr bool projected_fragments()
{
    const row owner {0, 8, {0, 1, 2, 3, 128, 255, 6, 7}};
    const datum::view_of view {owner};
    const std::array rows {view};
    using query = iv::query<offsets>;

    const auto wanted =
        iv::compile_intervals<2>(iv::set_union(query::closed_open(1, 3), query::closed_open(4, 6)));
    const iv::clip_view cuts {rows, wanted, [](const auto& value) {
                                  return iv::borrowed_interval<offsets> {
                                              iv::boundary_ref<offsets>::closed(value.first),
                                              iv::boundary_ref<offsets>::open(value.last)
                                          };
                              }};
    std::array<std::byte, 4> output {};
    const std::array fragments {std::span {owner.payload}.first(3),
                                std::span {owner.payload}.subspan(3)};
    datum::byte_cursor input {fragments};
    datum::resource_budget budget {{.payload_bytes = 4}};
    std::size_t copied = 0;
    for (const auto cut : cuts) {
        const auto lower = cut.bounds().lower().key();
        const auto length = cut.bounds().upper().key() - lower;
        budget.charge(datum::resource::payload_bytes, length);
        input.consume(lower - input.position());
        input.read_into(std::span {output}.subspan(copied, length));
        copied += length;
    }

    return output == std::array<std::byte, 4> {std::byte {1}, std::byte {2}, std::byte {128},
                                               std::byte {255}} and
           4 == copied and 6 == input.position() and
           4 == iv::measure(wanted, iv::unsigned_metric<unsigned> {});
}

constexpr bool retained_keys()
{
    auto key = datum::key::compose("record", std::string_view {"a\0b", 3});
    const auto original = std::string {datum::key::key_view(key)};
    datum::retained_view retained {datum::key::take_key(std::move(key)),
                                   datum::byte_slice {0, std::size(original)}};
    const auto moved = std::move(retained);
    const auto bytes = moved.get();
    const auto decoded = datum::key::decode<std::string, std::string>(
        std::string_view {std::data(bytes), std::size(bytes)});

    return decoded and "record" == std::get<0>(*decoded) and
           std::string_view {"a\0b", 3} == std::get<1>(*decoded);
}

} // anonymous namespace

static_assert(projected_fragments());
static_assert(retained_keys());

namespace {

constexpr bool failure_boundaries()
{
    std::array<char, 2> source {'a', 'b'};
    const std::array fragments {std::span {source}};
    datum::byte_cursor input {fragments};
    std::array<char, 3> output {'x', 'x', 'x'};
    try
     {
        input.read_into(std::span {output});

        return false;
     }
    catch (const std::length_error&)
     {
     }
    if (0 != input.position() || output != std::array {'x', 'x', 'x'}) {
        return false;
    }
    datum::resource_budget budget {{.depth = 1}};
    try
     {
        auto outer = budget.enter_depth();
        auto inner = budget.enter_depth();

        return false;
     }
    catch (const std::length_error&)
     {
     }
    if (0 != budget.used(datum::resource::depth)) {
        return false;
    }
    iv::workspace<offsets> scratch {{.work = 0}};
    using query = iv::query<offsets>;

    try
     {
        iv::evaluate(iv::difference(query::closed(0, 8), query::singleton(4)), scratch);

        return false;
     }
    catch (const std::length_error&)
     {
     }

    return 0 == scratch.retained_intervals() and 0 == scratch.work_performed();
}
} // anonymous namespace

static_assert(failure_boundaries());
static_assert(datum::render_detail::is_optional_v<const std::optional<int>&>);
static_assert(datum::render_detail::display_tuple<std::tuple<int, char>>);
static_assert(!datum::render_detail::is_optional_v<int>);

TEST_CASE("constant-evaluated pipelines agree with runtime operations", "[datum][constexpr]")
{
    CHECK(projected_fragments());
    CHECK(retained_keys());
    CHECK(failure_boundaries());
}

// Datum formatting tests: shared observations through Ceph's selected fmt target.
// Keep these fixtures neutral; application records belong in consumer tests.

namespace datum_fmt_test {

struct counted_record
{
    std::size_t *observations;
};
struct cycle
{
    const cycle *next;
};
struct record
{
    std::string name;
    std::int64_t revision;
    std::uint64_t count;
};
struct vocabulary
{
    std::tuple<double, std::chrono::milliseconds, std::chrono::duration<int, std::ratio<2, 3>>>
        values;
    std::map<std::string, float> entries;
};
struct enrolled_range final
{
    std::array<int, 2> payload {1, 2};
    std::size_t *fallback_observations;

public:
    auto begin() const
    {
        ++*fallback_observations;

        return std::begin(payload);
    }

    auto end() const { return std::end(payload); }

public:
    operator std::string_view() const
    {
        ++*fallback_observations;

        return "unreviewed";
    }

};
struct elapsed final : std::chrono::milliseconds
{
    using std::chrono::milliseconds::milliseconds;
};
struct flags final
{
    unsigned ready : 1;
    unsigned mode : 3;
};
struct accessor_record final
{
    int value;
    int& reference;
    mutable int counter;
    unsigned bits : 2;
    volatile int observed;
};
// Enrollment shields diagnostic values from unrelated ill-formed recognition.
struct guarded_record
{
    int value;

    using rep = int;
    using period = int;

public:
    template <typename ProbeT = void>

    auto begin() const
    {
        static_assert(!std::same_as<ProbeT, void>, "Do not inspect the unselected range interface");

        return std::addressof(value);
    }

    auto end() const { return std::addressof(value) + 1; }

};
struct guarded_projection final : guarded_record
{};
struct hidden_scalar final
{
    std::size_t *observations;
};
struct entry final
{
    std::string key;
    std::uint64_t version;
    std::string value;
    std::string access_token;
};
struct labeled_record final
{
    int number;
    int hidden;
};
template <typename ValueT>
struct selected_record final
{
    ValueT value;
};
} // datum_fmt_test

namespace ceph::salad::datum::render {

template <>
struct display_schema<datum_fmt_test::counted_record>
{
    static constexpr std::string_view name = "counted";
    static constexpr auto fields = std::tuple {field<[](const auto& value) {
        ++*value.observations;

        return 42;
    }>("value")};
};
template <>
struct display_projection<datum_fmt_test::cycle>
{
    static const auto& get(const datum_fmt_test::cycle& value) { return *value.next; }

};
template <>
struct display_schema<datum_fmt_test::record> : reflected_display_schema<^^datum_fmt_test::record, 3>
{};
template <>
struct display_schema<datum_fmt_test::vocabulary> : reflected_display_schema<^^datum_fmt_test::vocabulary, 2>
{};
template <>
struct display_schema<datum_fmt_test::flags> : reflected_display_schema<^^datum_fmt_test::flags, 2>
{};
template <>
struct display_schema<datum_fmt_test::hidden_scalar>
{
    static constexpr std::string_view name = "hidden";
    static constexpr auto accessor = [](const auto& value) -> int {
        ++*value.observations;
        throw std::runtime_error {"unnecessary observation"};
    };
    static constexpr auto fields =
        std::tuple {display_field<accessor, display_field_policy::redacted> {"value"}};
};
template <>
struct display_schema<datum_fmt_test::enrolled_range>
{
    static constexpr std::string_view name = "enrolled";
    static constexpr auto fields = std::tuple {
        display_field<&datum_fmt_test::enrolled_range::payload, display_field_policy::redacted> {
            "payload"}};
};
template <>
struct display_schema<datum_fmt_test::entry>
{
    static constexpr auto fields =
        std::tuple {field<&datum_fmt_test::entry::key>(), field<&datum_fmt_test::entry::version>(),
                    display_field<&datum_fmt_test::entry::value, display_field_policy::redacted>()};
};
template <>
struct display_schema<datum_fmt_test::labeled_record>
{
    static constexpr std::string_view name = "object";
    static constexpr auto fields = std::tuple {
        field<&datum_fmt_test::labeled_record::number>("visible"),
        display_field<&datum_fmt_test::labeled_record::hidden, display_field_policy::redacted> {
            "concealed"}};
};
template <typename ValueT>
struct display_schema<datum_fmt_test::selected_record<ValueT>>
{
    static constexpr auto fields =
        std::tuple {field<&datum_fmt_test::selected_record<ValueT>::value>()};
};
template <>
struct display_projection<datum_fmt_test::enrolled_range>
{
    static int get(const datum_fmt_test::enrolled_range& value)
    {
        ++*value.fallback_observations;

        return 99;
    }

};
template <>
struct display_schema<datum_fmt_test::guarded_record>
{
    static constexpr auto fields = std::tuple {field<&datum_fmt_test::guarded_record::value>()};
};
template <>
struct display_projection<datum_fmt_test::guarded_projection>
{
    static constexpr int get(const datum_fmt_test::guarded_projection& value)
    {
        return value.value;
    }

};
} // ceph::salad::datum::render

// Accessors preserve reference/cv categories, with a value only for bit fields.
template <typename RecordT, typename ResultT, std::size_t Index>
consteval bool reflected_accessor_returns()
{
    return []<auto Accessor, datum::render::display_field_policy Policy>(
               datum::render::display_field<Accessor, Policy>) {
        return std::same_as<std::invoke_result_t<decltype(Accessor), const RecordT&>, ResultT>;
    }(datum::render_detail::reflected_field<datum::detail::record_members<RecordT>[Index]>());
}
static_assert(reflected_accessor_returns<datum_fmt_test::accessor_record, const int&, 0>());
static_assert(reflected_accessor_returns<datum_fmt_test::accessor_record, int&, 1>());
static_assert(reflected_accessor_returns<datum_fmt_test::accessor_record, int&, 2>());
static_assert(reflected_accessor_returns<datum_fmt_test::accessor_record, unsigned, 3>());
static_assert(
    reflected_accessor_returns<datum_fmt_test::accessor_record, const volatile int&, 4>());
static_assert([] {
    std::string text;
    datum::render::format_to(std::back_inserter(text), datum_fmt_test::guarded_record {7});

    return text == "guarded_record{value=7}";
}());
static_assert([] {
    std::string text;
    datum::render::format_to(std::back_inserter(text), datum_fmt_test::guarded_projection {{7}});

    return text == "7";
}());

static_assert(std::output_iterator<fmt::appender, char>);
static_assert(std::formattable<datum_fmt_test::record, char>);
static_assert(fmt::is_formattable<datum_fmt_test::record>::value);

TEST_CASE("Datum fmt and standard formatting share character observations", "[datum][fmt]")
{
    const std::array values {datum_fmt_test::record {"", 0, 0},
                             datum_fmt_test::record {std::string {"a\0b", 3}, -1,
                                                     std::numeric_limits<std::uint64_t>::max()},
                             datum_fmt_test::record {"a\"b\\c\n", 41, 7},
                             datum_fmt_test::record {std::string(256, 'x'),
                                                     std::numeric_limits<std::int64_t>::min(), 42}};
    for (const auto& value : values) {
        std::string reference;
        datum::render::format_to(std::back_inserter(reference), value);
        CHECK(std::format("{}", value) == reference);
        CHECK(fmt::format("{}", value) == reference);

        fmt::memory_buffer buffer;
        fmt::format_to(fmt::appender {buffer}, "{}", value);
        CHECK(fmt::to_string(buffer) == reference);

        std::array<char, 1024> output;
        const auto last = datum::render::format_to(std::begin(output), value);
        CHECK(std::string(std::begin(output), last) == reference);
    }
}

TEST_CASE("explicit diagnostic tuples infer names without enrolling omitted members",
          "[datum][fmt]")
{
    const datum_fmt_test::entry value {"object/42", 7, "secret", "unapproved"};
    const std::string expected = "entry{key=\"object/42\", version=7, value=<redacted:6 bytes>}";
    CHECK(fmt::format("{}", value) == expected);
    CHECK(std::format("{}", value) == expected);
    std::string bounded;
    const auto result =
        datum::render::format_bounded(std::back_inserter(bounded), value, {.bytes = 35});
    CHECK(result.truncated);
    CHECK(result.written == 35);
    CHECK(bounded == expected.substr(0, 35));
    CHECK(fmt::format("{}", datum_fmt_test::selected_record<int> {42}) ==
          "selected_record{value=42}");
}

TEST_CASE("explicit record and field labels override diagnostic defaults", "[datum][fmt]")
{
    const datum_fmt_test::labeled_record value {3, 9};
    CHECK(fmt::format("{}", value) == "object{visible=3, concealed=<redacted>}");
    CHECK(std::format("{}", value) == "object{visible=3, concealed=<redacted>}");
}

static_assert([] {
    std::string output;
    datum::render::format_to(std::back_inserter(output), datum_fmt_test::selected_record<int> {42});

    return output == "selected_record{value=42}";
}());

TEST_CASE("minimal character sinks and total diagnostic budgets", "[datum][fmt][limits]")
{
    struct character_sink
    {
        std::string *result;

    public:
        auto& operator*() { return *this; }

        auto& operator++(int) { return *this; }
        void operator=(char value) { result->push_back(value); }

    };
    static_assert(datum::render_detail::character_output<character_sink>);
    static_assert(!std::output_iterator<character_sink, char>);
    const datum_fmt_test::record value {"abc", -42, 7};
    std::string text;
    datum::render::format_to(character_sink {&text}, value);
    CHECK(text == fmt::format("{}", value));
    ::ceph::util::clear(text);
    const auto limited =
        datum::render::format_bounded(character_sink {&text}, value, {.bytes = 12});
    CHECK(limited.truncated);
    CHECK(limited.written == 12);
    CHECK(std::size(text) == 12);
    CHECK(text == fmt::format("{}", value).substr(0, 12));
    datum_fmt_test::cycle cycle {};
    cycle.next = &cycle;
    ::ceph::util::clear(text);
    datum::render::format_to(character_sink {&text}, cycle, {.depth = 3});
    CHECK(std::empty(text));
    std::size_t observations = 0;
    const auto range = std::views::iota(0, 100) | std::views::transform([&](int n) {
                           ++observations;

                           return n;
                       });
    datum::render_detail::append_range(character_sink {&text}, range);
    CHECK(observations == 16);
    observations = 0;
    const auto binary = std::views::iota(0, 100) | std::views::transform([&](int n) {
                            ++observations;

                            return static_cast<std::byte>(n);
                        });
    datum::render_detail::append_binary(character_sink {&text}, binary);
    CHECK(observations == 32);
    observations = 0;
    ::ceph::util::clear(text);
    const datum_fmt_test::counted_record counted {&observations};
    const auto stopped =
        datum::render::format_bounded(character_sink {&text}, counted, {.bytes = 14});
    CHECK(stopped.truncated);
    CHECK(observations == 0);
}

TEST_CASE("standard vocabulary preserves units and bounded nested observations", "[datum][fmt]")
{
    const datum_fmt_test::vocabulary value {
        {1.25, std::chrono::milliseconds {3}, std::chrono::duration<int, std::ratio<2, 3>> {4}},
        {{"key", 2.5F}}};
    const auto text = fmt::format("{}", value);
    CHECK(text == "vocabulary{values=(1.25, 3ms, 4[2/3]s), entries=[(\"key\", 2.5)]}");
    CHECK(std::format("{}", value) == text);
    std::string bounded;
    const auto result =
        datum::render::format_bounded(std::back_inserter(bounded), value, {.bytes = 20});
    CHECK(result.truncated);
    CHECK(bounded == text.substr(0, 20));
    ::ceph::util::clear(bounded);
    datum::render::format_to(std::back_inserter(bounded), value, {.depth = 1});
    CHECK(bounded == "vocabulary{values=, entries=}");
}

TEST_CASE("explicit diagnostic enrollment wins over range text and projection recognition",
          "[datum][fmt]")
{
    std::size_t observations = 0;
    const datum_fmt_test::enrolled_range value {{1, 2}, &observations};
    CHECK(fmt::format("{}", value) == "enrolled{payload=<redacted:2 elements>}");
    CHECK(std::format("{}", value) == "enrolled{payload=<redacted:2 elements>}");
    CHECK(0 == observations);
}

TEST_CASE("time diagnostics preserve units without overflowing a nanosecond conversion",
          "[datum][fmt]")
{
    std::string output;
    using duration = std::chrono::hours;
    using point = std::chrono::time_point<std::chrono::system_clock, duration>;

    datum::render_detail::append_value(
        std::back_inserter(output), point {duration {std::numeric_limits<duration::rep>::max()}});
    CHECK(output == std::to_string(std::numeric_limits<duration::rep>::max()) + "h");
    ::ceph::util::clear(output);
    datum::render_detail::append_value(std::back_inserter(output), datum_fmt_test::elapsed {3});
    CHECK(output == "3ms");
}

static_assert([] {
    std::string output;
    datum::render::format_to(std::back_inserter(output), datum_fmt_test::flags {1, 5});

    return output == "flags{ready=1, mode=5}";
}());

TEST_CASE("reflected diagnostics observe bit fields as values", "[datum][fmt]")
{
    CHECK(fmt::format("{}", datum_fmt_test::flags {1, 5}) == "flags{ready=1, mode=5}");
}

TEST_CASE("redacting a scalar skips its unneeded accessor", "[datum][fmt]")
{
    std::size_t observations = 0;
    CHECK(fmt::format("{}", datum_fmt_test::hidden_scalar {&observations}) ==
          "hidden{value=<redacted>}");
    CHECK(0 == observations);
}

namespace interval = ceph::salad::datum::interval;

struct integer_domain final : interval::comparison_domain<int>
{
    static constexpr std::optional<int> prefix_upper_bound(const int value) noexcept
    {
        if (100 == value) {
            return std::nullopt;
        }

        return 1 + value;
    }

};

using integer_query = interval::query<integer_domain>;

struct dense_domain final : interval::comparison_domain<int>
{};

using dense_query = interval::query<dense_domain>;

struct tagged_value final
{
    int value = 0;

public:
    constexpr auto operator<=>(const tagged_value&) const noexcept = default;

};

struct tagged_domain final : interval::comparison_domain<tagged_value>
{
    static constexpr std::optional<tagged_value> prefix_upper_bound(
        const tagged_value value) noexcept
    {
        if (10 == value.value) {
            return std::nullopt;
        }

        return tagged_value {.value = 1 + value.value};
    }

};

using tagged_query = interval::query<tagged_domain>;

static_assert(ceph::salad::datum::render::displayable<integer_query>);
static_assert(ceph::salad::datum::render::displayable<interval::lower_endpoint<integer_domain>>);
static_assert(
    ceph::salad::datum::render::displayable<interval::static_interval_set<integer_domain, 2>>);
static_assert(ceph::salad::datum::render::displayable<
              decltype(interval::without(integer_query::closed_open(1, 9), 5))>);
#ifdef DATUM_HAVE_ZPP
 static_assert(!ceph::salad::datum::serializable<integer_query>);
 static_assert(
     !ceph::salad::datum::serializable<interval::static_interval_set<integer_domain, 2>>);
#endif

struct projected_integer_interval final
{
    using domain_type = integer_domain;
    using boundary_type = interval::boundary<integer_domain>;

public:
    static constexpr projected_integer_interval closed_open(const int begin_key, const int end_key)
    {
        return {.begin_key = begin_key, .end_key = end_key};
    }

public:
    static constexpr projected_integer_interval empty() { return closed_open(7, 7); }

public:
    constexpr boundary_type lower() const { return boundary_type::closed(begin_key); }

    constexpr boundary_type upper() const { return boundary_type::open(end_key); }

    int begin_key = 0;
    int end_key = 0;

};

struct counted_projected_interval final
{
    using domain_type = integer_domain;
    using boundary_type = interval::boundary<integer_domain>;

public:
    boundary_type lower() const
    {
        ++lower_calls;

        return boundary_type::closed(begin_key);
    }

    boundary_type upper() const
    {
        ++upper_calls;

        return boundary_type::open(end_key);
    }

    int begin_key = 0;
    int end_key = 0;

    mutable std::size_t lower_calls = 0;
    mutable std::size_t upper_calls = 0;

};

struct boundary_recording_sink final
{
    std::vector<integer_query> intervals;
    std::size_t boundary_emissions = 0;
    std::size_t interval_emissions = 0;

public:
    template <interval::boundary_view_for<integer_domain> LowerT,
              interval::boundary_view_for<integer_domain> UpperT>

    void emit_interval(const LowerT& lower, const UpperT& upper)
    {
        ++boundary_emissions;
        ::ceph::util::push_back(intervals, integer_query::between(lower, upper));
    }

public:
    template <interval::interval_view IntervalT>

    void operator()(const IntervalT& interval)
    {
        ++interval_emissions;
        ::ceph::util::push_back(intervals,
                                integer_query::between(interval.lower(), interval.upper()));
    }

};

constexpr bool contains(const integer_query& query, const int value)
{
    return interval::contains(query, value);
}

constexpr bool equivalent_on_samples(const auto& lhs, const auto& rhs, const auto& samples)
{
    return std::ranges::all_of(samples, [&](const auto& value) {
        return interval::contains(lhs, value) == interval::contains(rhs, value);
    });
}

constexpr bool static_compiler_accepts_projected_intervals()
{
    constexpr auto compiled =
        interval::compile_intervals<1>(projected_integer_interval::closed_open(2, 8));

    return 1 == std::size(compiled) and integer_query::closed_open(2, 8) == compiled[0];
}

constexpr bool static_compiler_coalesces_intervals()
{
    constexpr auto compiled = interval::compile_intervals<1>(
        interval::set_union(integer_query::closed_open(1, 5), integer_query::closed_open(3, 8)));

    return 1 == std::size(compiled) and integer_query::closed_open(1, 8) == compiled[0];
}

constexpr bool static_compiler_orders_disjoint_intervals()
{
    constexpr auto compiled = interval::compile_intervals<2>(interval::set_union(
        integer_query::closed_open(40, 50), integer_query::closed_open(10, 20)));

    return 2 == std::size(compiled) and integer_query::closed_open(10, 20) == compiled[0] and
           integer_query::closed_open(40, 50) == compiled[1];
}

constexpr bool static_compiler_clips_composite_intervals()
{
    constexpr auto compiled = interval::compile_intervals<2>(
        interval::intersection(integer_query::closed_open(15, 45),
                               interval::set_union(integer_query::closed_open(10, 20),
                                                   integer_query::closed_open(40, 50))));

    return 2 == std::size(compiled) and integer_query::closed_open(15, 20) == compiled[0] and
           integer_query::closed_open(40, 45) == compiled[1];
}

constexpr bool static_compiler_normalizes_nested_union()
{
    constexpr auto compiled = interval::compile_intervals<2>(interval::set_union(
        interval::set_union(integer_query::closed_open(40, 50), integer_query::closed_open(10, 20)),
        integer_query::closed_open(15, 45)));

    return 1 == std::size(compiled) and integer_query::closed_open(10, 50) == compiled[0];
}

constexpr bool helper_layer_is_compile_time_capable()
{
    constexpr auto page =
        interval::ending_before(interval::starting_after(integer_query::closed_open(1, 10), 3), 8);
    constexpr auto without_marker = interval::without(page, 5);
    constexpr auto compiled = interval::compile_intervals<2>(without_marker);

    return 2 == std::size(compiled) and integer_query::open(3, 5) == compiled[0] and
           integer_query::open(5, 8) == compiled[1] and
           interval::intersects(integer_query::closed_open(1, 5),
                                integer_query::closed_open(4, 8)) and
           interval::is_disjoint(integer_query::closed_open(1, 5),
                                 integer_query::closed_open(5, 8)) and
           interval::encloses(integer_query::closed_open(1, 10), integer_query::closed_open(3, 8));
}

constexpr bool helper_predicates_accept_expressions()
{
    constexpr auto lhs =
        interval::set_union(integer_query::closed_open(1, 4), integer_query::closed_open(8, 10));
    constexpr auto rhs =
        interval::difference(integer_query::closed_open(3, 9), integer_query::closed_open(5, 7));
    constexpr auto inner = interval::intersection(lhs, integer_query::closed_open(8, 9));

    return interval::intersects(lhs, rhs) and !interval::is_disjoint(lhs, rhs) and
           interval::encloses(lhs, inner) and
           !interval::encloses(integer_query::closed_open(1, 4), rhs);
}

constexpr bool helpers_accept_projected_interval_views()
{
    constexpr auto clipped = interval::ending_at(
        interval::starting_after(projected_integer_interval::closed_open(2, 8), 4), 6);
    constexpr auto compiled = interval::compile_intervals<1>(clipped);

    return 1 == std::size(compiled) and
           integer_query::between(interval::open_bound<integer_domain>(4),
                                  interval::closed_bound<integer_domain>(6)) == compiled[0];
}

constexpr bool helpers_are_generic_over_ordered_domains()
{
    constexpr auto prefix = interval::prefix<tagged_domain>(tagged_value {3});
    constexpr auto page = interval::ending_before(
        interval::starting_at(tagged_query::closed_open(tagged_value {1}, tagged_value {8}), tagged_value {3}),
        tagged_value {6});
    constexpr auto without_marker = interval::without(page, tagged_value {4});
    constexpr auto without_nested = interval::without_prefix(
        tagged_query::closed_open(tagged_value {1}, tagged_value {8}), tagged_value {3});
    constexpr auto clipped =
        interval::between(interval::lower_after<tagged_domain>(tagged_value {3}),
                          interval::upper_at_or_before<tagged_domain>(tagged_value {4}));
    constexpr auto compiled = interval::compile_intervals<2>(without_marker);
    constexpr auto prefix_page =
        interval::prefix_starting_after<tagged_domain>(tagged_value {3}, tagged_value {3});

    return tagged_query::closed_open(tagged_value {3}, tagged_value {4}) == prefix and
           tagged_query::between(interval::open_bound<tagged_domain>(tagged_value {3}),
                                 interval::closed_bound<tagged_domain>(tagged_value {4})) ==
               clipped and
           tagged_query::closed_open(tagged_value {3}, tagged_value {6}) == page and
           tagged_query::closed_open(tagged_value {3}, tagged_value {4}) == compiled[0] and
           tagged_query::open(tagged_value {4}, tagged_value {6}) == compiled[1] and
           tagged_query::open(tagged_value {3}, tagged_value {4}) == prefix_page and
           2 == interval::interval_count(without_nested);
}

constexpr bool dense_domains_do_not_need_prefix_bounds()
{
    constexpr auto query =
        interval::intersection(dense_query::closed_open(1, 10), interval::after<dense_domain>(4));
    constexpr auto compiled = interval::compile_intervals<1>(query);

    return 1 == std::size(compiled) and dense_query::open(4, 10) == compiled[0];
}

static_assert(interval::ordered_domain<integer_domain>);
static_assert(interval::ordered_domain<dense_domain>);
static_assert(!interval::prefix_domain<dense_domain>);
static_assert(interval::interval_view<projected_integer_interval>);
static_assert(interval::interval_view<counted_projected_interval>);
static_assert(interval::expression<integer_query>);
static_assert(interval::expression<projected_integer_interval>);
static_assert(interval::canonical_interval<integer_query>);
static_assert(!interval::canonical_interval<projected_integer_interval>);
static_assert(!std::same_as<projected_integer_interval, integer_query>);
static_assert(interval::expression<decltype(interval::set_union(integer_query::empty(),
                                                                integer_query::universal()))>);

static_assert(contains(integer_query::closed(2, 4), 2));
static_assert(contains(integer_query::closed(2, 4), 4));
static_assert(!contains(integer_query::open(2, 4), 2));
static_assert(!contains(integer_query::open(2, 4), 4));
static_assert(interval::is_empty(integer_query::closed_open(7, 7)));
static_assert(!interval::is_empty(integer_query::singleton(7)));
static_assert(interval::contains(projected_integer_interval::closed_open(2, 4), 2));
static_assert(static_compiler_accepts_projected_intervals());
static_assert(static_compiler_coalesces_intervals());
static_assert(static_compiler_orders_disjoint_intervals());
static_assert(static_compiler_clips_composite_intervals());
static_assert(static_compiler_normalizes_nested_union());
static_assert(helper_layer_is_compile_time_capable());
static_assert(helper_predicates_accept_expressions());
static_assert(helpers_accept_projected_interval_views());
static_assert(helpers_are_generic_over_ordered_domains());
static_assert(dense_domains_do_not_need_prefix_bounds());

TEST_CASE("Datum interval expressions display without compilation", "[datum][interval][display]")
{
    const auto selection = interval::without(integer_query::closed_open(1, 9), 5);
    const auto displayed = std::format("{}", selection);

    CHECK(std::string::npos != displayed.find("interval_difference{"));
    CHECK(std::string::npos !=
          displayed.find("lower=interval_boundary{kind=finite, key=1, inclusive=true}"));
    CHECK(std::string::npos !=
          displayed.find("upper=interval_boundary{kind=finite, key=9, inclusive=false}"));
    CHECK(std::string::npos !=
          displayed.find("interval{lower=interval_boundary{kind=finite, key=5"));

    const auto combined = interval::set_union(integer_query::closed_open(1, 3),
                                              integer_query::closed_open(5, 9));
    CHECK(std::format("{}", combined).starts_with("interval_union{lhs=interval{"));
    const auto overlap = interval::intersection(combined, integer_query::closed_open(2, 7));
    CHECK(std::format("{}", overlap).starts_with("interval_intersection{lhs=interval_union{"));
    const auto compiled = interval::compile_intervals<2>(selection);

    CHECK(std::string::npos !=
          std::format("{}", compiled).find("interval_set{intervals=[interval{"));
}

TEST_CASE("Datum interval primitives describe generic ordered domains", "[datum][interval]")
{
    CHECK(interval::closed_bound<integer_domain>(3).inclusive());
    CHECK(!interval::open_bound<integer_domain>(3).inclusive());

    CHECK(interval::is_empty(integer_query::empty()));
    CHECK(!interval::is_empty(integer_query::universal()));
    CHECK(!interval::is_empty(integer_query::singleton(7)));

    CHECK(1 == std::size(interval::compile_intervals(integer_query::closed_open(2, 5))));
    CHECK(0 == std::size(interval::compile_intervals(integer_query::closed_open(7, 7))));
}

TEST_CASE("Datum interval algorithms accept foreign interval views", "[datum][interval]")
{
    constexpr auto foreign = projected_integer_interval::closed_open(2, 8);
    constexpr auto empty = projected_integer_interval::empty();
    counted_projected_interval counted {.begin_key = 2, .end_key = 8};

    CHECK(interval::contains(foreign, 2));
    CHECK(!interval::contains(foreign, 8));
    CHECK(interval::is_empty(empty));
    CHECK(interval::contains(counted, 2));
    CHECK(1 == counted.lower_calls);
    CHECK(1 == counted.upper_calls);

    counted.lower_calls = 0;
    counted.upper_calls = 0;

    CHECK(integer_query::closed_open(2, 8) == interval::copy_interval(counted));
    CHECK(1 == counted.lower_calls);
    CHECK(1 == counted.upper_calls);

    counted.lower_calls = 0;
    counted.upper_calls = 0;

    interval::for_each_interval(counted, [](const auto&) {});
    CHECK(1 == counted.lower_calls);
    CHECK(1 == counted.upper_calls);

    CHECK(std::ranges::equal(interval::compile_intervals(
              interval::intersection(foreign, integer_query::closed_open(4, 10))),
          std::vector {integer_query::closed_open(4, 8)}));
    CHECK_THROWS_AS(interval::compile_intervals<1>(interval::set_union(
                        integer_query::closed_open(1, 2), integer_query::closed_open(4, 5))), std::length_error);

    CHECK(0 == std::size(interval::compile_intervals(
                   interval::intersection(integer_query::empty(), foreign))));
    CHECK(0 == interval::interval_count(interval::intersection(integer_query::empty(), foreign)));

    CHECK(std::ranges::equal(interval::compile_intervals(
              interval::set_union(foreign, integer_query::closed_open(8, 12))),
          std::vector {integer_query::closed_open(2, 12)}));

    CHECK(std::ranges::equal(interval::compile_intervals(
              interval::difference(foreign, integer_query::closed_open(4, 6))),
          std::vector {integer_query::closed_open(2, 4), integer_query::closed_open(6, 8)}));

    boundary_recording_sink sink;
    interval::for_each_interval(interval::difference(foreign, integer_query::closed_open(4, 6)), sink);
    CHECK(2 == sink.boundary_emissions);
    CHECK(0 == sink.interval_emissions);
    CHECK(sink.intervals ==
          std::vector {integer_query::closed_open(2, 4), integer_query::closed_open(6, 8)});

    CHECK(std::ranges::equal(interval::compile_intervals(interval::complement(foreign)),
          std::vector {
              integer_query::between(interval::boundary<integer_domain>::negative_infinity(),
                                     interval::open_bound<integer_domain>(2)),
              integer_query::between(interval::closed_bound<integer_domain>(8),
                                     interval::boundary<integer_domain>::positive_infinity())}));
}

TEST_CASE("Datum interval prefix queries use the domain's prefix upper bound", "[datum][interval]")
{
    const auto integers = integer_query::prefix(41);
    const auto max_prefix = integer_query::prefix(100);

    CHECK(contains(integers, 41));
    CHECK(!contains(integers, 42));

    CHECK(contains(max_prefix, 100));
    CHECK(iv::is_above_all(max_prefix.upper()));
}

TEST_CASE("Datum interval helpers build common query shapes", "[datum][interval]")
{
    CHECK(interval::endpoint_inclusion::included ==
          interval::lower_at_or_after<integer_domain>(7).inclusion);
    CHECK(interval::endpoint_inclusion::excluded ==
          interval::lower_after<integer_domain>(7).inclusion);
    CHECK(interval::endpoint_inclusion::included ==
          interval::upper_at_or_before<integer_domain>(7).inclusion);
    CHECK(interval::endpoint_inclusion::excluded ==
          interval::upper_before<integer_domain>(7).inclusion);

    CHECK(integer_query::singleton(7) == interval::at<integer_domain>(7));
    CHECK(interval::after<integer_domain>(7) ==
          interval::from(interval::lower_after<integer_domain>(7)));
    CHECK(interval::before<integer_domain>(7) ==
          interval::until(interval::upper_before<integer_domain>(7)));
    CHECK(integer_query::closed_open(2, 5) == interval::between<integer_domain>(2, 5));
    CHECK(integer_query::closed(2, 5) == interval::closed_between<integer_domain>(2, 5));
    CHECK(integer_query::open(2, 5) == interval::open_between<integer_domain>(2, 5));
    CHECK(interval::between(interval::lower_after<integer_domain>(2),
                            interval::upper_at_or_before<integer_domain>(5)) ==
          integer_query::between(interval::open_bound<integer_domain>(2), interval::closed_bound<integer_domain>(5)));

    const auto page =
        interval::ending_before(interval::starting_at(integer_query::closed_open(1, 10), 3), 8);

    CHECK(integer_query::closed_open(3, 8) == page);
    CHECK(integer_query::open(3, 8) == interval::starting_after(page, 3));
    CHECK(integer_query::closed(3, 6) == interval::ending_at(page, 6));
}

TEST_CASE("Datum interval helper predicates describe interval relations", "[datum][interval]")
{
    const auto low = integer_query::closed_open(1, 5);
    const auto touching = integer_query::closed_open(5, 8);
    const auto overlapping = integer_query::closed_open(4, 8);
    const auto inner = integer_query::closed_open(2, 4);
    const auto split =
        interval::set_union(integer_query::closed_open(1, 3), integer_query::closed_open(7, 9));

    CHECK(interval::is_disjoint(low, touching));
    CHECK(!interval::intersects(low, touching));
    CHECK(interval::intersects(low, overlapping));
    CHECK(interval::encloses(low, inner));
    CHECK(!interval::encloses(inner, low));
    CHECK(interval::encloses(low, integer_query::empty()));
    CHECK(interval::encloses(split, integer_query::closed_open(7, 8)));
    CHECK(!interval::encloses(split, integer_query::closed_open(2, 8)));
}

TEST_CASE("Datum interval set operations emit canonical intervals", "[datum][interval]")
{
    CHECK(std::ranges::equal(
        interval::compile_intervals(interval::intersection(integer_query::closed_open(1, 10),
                                                          integer_query::closed_open(4, 20))),
        std::vector {integer_query::closed_open(4, 10)}));

    CHECK(std::ranges::equal(
        interval::compile_intervals(interval::difference(integer_query::closed_open(1, 10),
                                                        integer_query::closed_open(4, 7))),
        std::vector {integer_query::closed_open(1, 4), integer_query::closed_open(7, 10)}));
    CHECK(std::ranges::equal(
        interval::compile_intervals(interval::difference(integer_query::open(1, 10),
                                                        integer_query::singleton(7))),
        std::vector {integer_query::open(1, 7), integer_query::open(7, 10)}));

    CHECK(std::ranges::equal(
        interval::compile_intervals(interval::set_union(integer_query::closed_open(1, 4),
                                                       integer_query::closed_open(4, 8))),
        std::vector {integer_query::closed_open(1, 8)}));
    CHECK(std::ranges::equal(
        interval::compile_intervals(interval::set_union(integer_query::closed_open(40, 50),
                                                       integer_query::closed_open(10, 20))),
        std::vector {integer_query::closed_open(10, 20), integer_query::closed_open(40, 50)}));
    CHECK(std::ranges::equal(
        interval::compile_intervals(interval::set_union(integer_query::closed_open(1, 5),
                                                       integer_query::closed_open(3, 8))),
        std::vector {integer_query::closed_open(1, 8)}));

    CHECK(std::ranges::equal(
        interval::compile_intervals(interval::intersection(
            integer_query::closed_open(15, 45),
            interval::set_union(integer_query::closed_open(10, 20), integer_query::closed_open(40, 50)))),
        std::vector {integer_query::closed_open(15, 20), integer_query::closed_open(40, 45)}));
    CHECK(2 == interval::interval_count(interval::intersection(
                   integer_query::closed_open(15, 45),
                   interval::set_union(integer_query::closed_open(10, 20),
                                       integer_query::closed_open(40, 50)))));
}

TEST_CASE("Datum interval expressions satisfy sampled algebra laws", "[datum][interval]")
{
    constexpr auto samples = std::array {-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    constexpr auto a = integer_query::closed_open(1, 7);
    constexpr auto b = integer_query::closed_open(3, 9);
    constexpr auto empty = integer_query::empty();
    constexpr auto universal = integer_query::universal();

    static_assert(equivalent_on_samples(interval::intersection(a, universal), a, samples));
    static_assert(equivalent_on_samples(interval::set_union(a, empty), a, samples));
    static_assert(equivalent_on_samples(interval::difference(a, a), empty, samples));

    CHECK(
        equivalent_on_samples(interval::intersection(a, b), interval::intersection(b, a), samples));
    CHECK(equivalent_on_samples(interval::set_union(a, b), interval::set_union(b, a), samples));
    CHECK(equivalent_on_samples(
        interval::complement(interval::set_union(a, b)),
        interval::intersection(interval::complement(a), interval::complement(b)), samples));
}

// Datum data tests: neutral record enrollment, ownership, field codecs and sinks.
// Keep domain profiles in these fixtures, outside the shared mechanisms.

namespace datum_test {

struct record
{
    std::string name;
    std::uint64_t version;

public:
    bool operator==(const record&) const = default;

};
struct collision_record
{
    int project;
    int record_type;
};
class constructed_record final
{
    [[maybe_unused]] int hidden = 3;

public:
    int value;

public:
    constexpr explicit constructed_record(const int initial) : value(initial)
    {}

};
struct borrowed_record
{
    std::string_view name;
};
struct ticks
{
    std::int64_t count;
};
struct failing_field final
{
    int value = 0;
};
struct clock_record
{
    ticks value;
};
struct scalar_record
{
    int first;

    int second;
};
struct entry final
{
    std::string key;
    std::uint64_t version = 99;
    std::string value;

public:
    bool operator==(const entry&) const = default;

};
struct local_entry final
{
    std::string key;
    std::uint64_t version = 99;
    std::string value;

public:
    std::function<void()> on_change;

};
struct record_with_schema_names final
{
    using record_type = scalar_record;
    using fields = datum::binary_fields<&scalar_record::first>;

    int value;
};
static_assert(!datum::binary_schema_type<record_with_schema_names>);
static_assert(!datum::binary_schema_type<datum::binary_fields<>>);
static_assert(datum::binary_schema_type<datum::binary_fields<&borrowed_record::name>>);
static_assert(!datum::binary_schema_type<datum::binary_fields<&entry::key, &entry::key>>);
static_assert(!datum::binary_schema_type<datum::binary_fields<&entry::key, &scalar_record::first>>);
static_assert(std::same_as<datum::binary_schema_t<scalar_record>,
                           datum::binary_fields<&scalar_record::first, &scalar_record::second>>);
static_assert(std::same_as<datum::binary_schema_t<const scalar_record,
                                                 &scalar_record::first>::record_type,
                           const scalar_record>);
static_assert(std::same_as<datum::binary_schema_t<const scalar_record>,
                           datum::binary_schema_t<const scalar_record, &scalar_record::first,
                                                  &scalar_record::second>>);

class private_record final
{
    int hidden;

public:
    static constexpr auto selection() { return datum::binary_fields<&private_record::hidden> {}; }

};
static_assert(!datum::binary_schema_type<decltype(private_record::selection())>);

struct derived_record final : scalar_record
{
    int local;
};
static_assert(!datum::binary_schema_type<datum::binary_fields<&derived_record::local>>);
// A member pointer identifies its declaring class, including inherited spelling.
static_assert(std::same_as<datum::binary_fields<&derived_record::first>::record_type,
                           scalar_record>);

struct projected_text
{
    std::string text;
};
struct projected_record
{
    projected_text value;
};
struct projected_scalar
{
    int value;
};
struct scalar_projection_record
{
    projected_scalar value;
};
struct fallible_projection
{
    int value;
    bool fail;
};
struct fallible_projection_record
{
    fallible_projection value;
};
struct moving_projection
{
    int value;
    bool fail;

public:
    constexpr moving_projection(int number, bool failing) noexcept : value(number), fail(failing)
    {}
    constexpr moving_projection(moving_projection&& source) : value(source.value), fail(source.fail)
    {
        if (fail) {
            throw std::runtime_error {"projection move failed"};
        }
    }

};
struct move_projection_source
{
    int value;
    bool fail;
};
struct move_projection_record
{
    move_projection_source value;
};
struct immovable_projection final
{
    int value;

public:
    constexpr explicit immovable_projection(const int initial) noexcept : value(initial)
    {}
    immovable_projection(const immovable_projection&) = delete;
    immovable_projection(immovable_projection&&) = delete;

};
struct immovable_record final
{
    move_projection_source value;
};
enum struct label { first = 2, alias = 2, last = 9 };
enum struct flag : bool { set = true };
enum struct letter : char { first = 1 };
} // datum_test

namespace ceph::salad::datum {

template <>
struct member_view<datum_test::projected_text>
{
    using type = std::string_view;

public:
    static constexpr type get(const datum_test::projected_text& value) noexcept
    {
        return value.text;
    }

};
template <>
struct member_view<datum_test::projected_scalar>
{
    using type = int;

public:
    static constexpr type get(const datum_test::projected_scalar& value) noexcept
    {
        return value.value;
    }

};
template <>
struct member_view<datum_test::fallible_projection>
{
    using type = int;

public:
    static constexpr type get(const datum_test::fallible_projection& value)
    {
        if (value.fail) {
            throw std::runtime_error {"projection failed"};
        }

        return value.value;
    }

};
template <>
struct member_view<datum_test::move_projection_source>
{
    using type = datum_test::moving_projection;

public:
    static constexpr type get(const datum_test::move_projection_source& value) noexcept
    {
        return {value.value, value.fail};
    }

};
template <>
struct member_view<datum_test::immovable_record>
{
    using type = datum_test::immovable_projection;

public:
    static constexpr type get(const datum_test::immovable_record& value) noexcept
    {
        return type {value.value.value};
    }

};
} // ceph::salad::datum

namespace ceph::salad::datum::render {

template <>
struct enum_schema<datum_test::label> : reflected_enum_schema<datum_test::label>
{};
template <>
struct enum_schema<datum_test::flag> : reflected_enum_schema<datum_test::flag>
{};
template <>
struct enum_schema<datum_test::letter> : reflected_enum_schema<datum_test::letter>
{};
template <>
struct display_schema<datum_test::record> : reflected_display_schema<^^datum_test::record, 2>
{};
} // ceph::salad::datum::render

namespace ceph::salad::datum {

template <>
struct binary_schema<datum_test::record>
    : checked_binary_schema<datum_test::record, &datum_test::record::name,
                            &datum_test::record::version>
{};
#ifdef DATUM_HAVE_ZPP
 template <>
 struct binary_field_codec<datum_test::ticks>
 {
     static constexpr auto apply(auto& archive, auto& value) { return archive(value.count); }

 };
 template <>
 struct binary_field_codec<datum_test::failing_field>
 {
     static zpp::bits::errc apply(auto&, auto&)
     {
         throw std::system_error {std::make_error_code(std::errc::permission_denied),
                                  "custom field representation"};
     }

 };
#endif
template <>
struct binary_schema<datum_test::clock_record>
    : checked_binary_schema<datum_test::clock_record, &datum_test::clock_record::value>
{};
template <>
struct binary_schema<datum_test::scalar_record>
    : checked_binary_schema<datum_test::scalar_record, &datum_test::scalar_record::second,
                            &datum_test::scalar_record::first>
{};
} // ceph::salad::datum

#ifdef DATUM_HAVE_ZPP
 namespace datum_test {

 template <typename T>
 requires datum::binary_record<T>
 auto serialize(const T&) -> zpp::bits::members<datum::binary_member_count<T>>;

 template <typename ArchiveT, typename T>
 requires datum::binary_record<T>
 constexpr auto serialize(ArchiveT& archive, T& value)
 {
     return datum::serialize_schema(
         archive, value, typename datum::binary_schema<std::remove_cvref_t<T>>::fields {});
 }
 } // datum_test

#endif

using scalar_fields = datum::binary_schema<datum_test::scalar_record>::fields;

template <typename ValueT, typename SinkT>
concept observable_scalar = requires(const ValueT& value, SinkT&& sink) {
    scalar_fields::observe(value, std::forward<SinkT>(sink));
};
static_assert(!observable_scalar<datum_test::collision_record, decltype([](const auto&) {})>);
static_assert(!observable_scalar<datum_test::scalar_record, decltype([](int) { return true; })>);
using first_lens = datum::member_lens<&datum_test::scalar_record::first>;

struct incomplete_record;
static_assert(!std::invocable<first_lens, const incomplete_record&>);
static_assert(std::invocable<first_lens, const datum_test::scalar_record&>);
static_assert(!std::invocable<first_lens, datum_test::scalar_record&&>);
static_assert(!std::invocable<first_lens, const datum_test::scalar_record&&>);
static_assert(!std::invocable<first_lens, datum::view_of<datum_test::scalar_record>&&>);
using scalar_projection_lens = datum::member_lens<&datum_test::scalar_projection_record::value>;
// Query the projection before constructing a view: deduction must complete it.
static_assert(
    std::same_as<std::invoke_result_t<scalar_projection_lens,
                                      const datum::view_of<datum_test::scalar_projection_record>&>,
                 const int&>);
static_assert([] {
    const datum_test::scalar_record source {3, 7};
    struct named_view : datum::view_of<datum_test::scalar_record>
    {
        using view_of::view_of;
    };
    const named_view view {source};
    std::array<int, 2> values {};
    auto next = std::begin(values);
    scalar_fields::observe(view, [&next](const int value) { *next++ = value; });
    constexpr datum::member_lens<&datum_test::scalar_record::first> first;
    static_assert(std::same_as<decltype(first(view)), const int&>);

    return values == std::array {7, 3} and &first(view) == &source.first;
}());

TEST_CASE("Datum selected fields preserve schema order and owner/view association", "[datum][data]")
{
    datum_test::scalar_record source {3, 7};
    const datum::view_of view {source};
    std::array<int, 2> values {};
    scalar_fields::observe(view, [next = std::begin(values), token = std::make_unique<int>(1)](
                                     const int field) mutable { *next++ = *token * field; });
    CHECK(values == std::array {7, 3});
    source.first = 11;
    CHECK(datum::member_lens<&datum_test::scalar_record::first> {}(view) == 11);
    CHECK(&datum::member_lens<&datum_test::scalar_record::first> {}(view) == &source.first);
#ifdef DATUM_HAVE_ZPP
     CHECK_FALSE(datum::serializable<decltype(view)>);

     const datum_test::clock_record custom {{-123}};
     const datum::view_of custom_view {custom};
     datum::binary_buffer observed;
     zpp::bits::out output {observed};
     datum::binary_schema<datum_test::clock_record>::fields::observe(
         custom_view,
         [&output](const auto& field) { datum::serialize_field(output, field).or_throw(); });
     CHECK(observed == datum::to_bytes(custom));
#endif
}

TEST_CASE("Datum field lenses retain a consumer's value projections", "[datum][data]")
{
    const datum_test::projected_record source {{"projected"}};
    const datum::view_of view {source};
    constexpr datum::member_lens<&datum_test::projected_record::value> lens;
    static_assert(std::same_as<decltype(lens(source)), const datum_test::projected_text&>);
    static_assert(std::same_as<decltype(lens(view)), const std::string_view&>);
    CHECK(lens(view) == "projected");
    CHECK(std::data(lens(view)) == std::data(source.value.text));
    CHECK(&lens(view) == &view.value);

    struct mutable_record
    {
        mutable int value;
    };
    const mutable_record counter {7};
    constexpr datum::member_lens<&mutable_record::value> counter_lens;
    static_assert(std::same_as<decltype(counter_lens(counter)), const int&>);
    CHECK(&counter_lens(counter) == &counter.value);
}

TEST_CASE("Datum ordered lenses allow policy before observation and propagate consumer failures",
          "[datum][data]")
{
    struct record
    {
        int value;
        unsigned& reads;
    };
    constexpr auto observe = [](const record& value) {
        ++value.reads;

        return value.value;
    };
    using fields = datum::field_order<observe>;

    unsigned reads = 0;
    const record source {7, reads};
    fields::visit([](const auto&) {});
    CHECK(reads == 0);
    fields::visit([&source](const auto& lens) { CHECK(std::invoke(lens, source) == 7); });
    CHECK(reads == 1);

    const datum_test::scalar_record ordered {3, 7};
    unsigned calls = 0;
    CHECK_THROWS_AS(scalar_fields::observe(ordered,
                                           [&calls](int) {
                                               ++calls;
                                               throw std::runtime_error {"consumer failed"};
                                           }),
                    std::runtime_error);
    CHECK(calls == 1);
    using empty_fields = datum::field_order<>;

    empty_fields::observe(ordered, [&calls](int) { ++calls; });
    CHECK(calls == 1);
}

TEST_CASE("generated views construct projections directly without intermediate moves",
          "[datum][view]")
{
    using R = datum_test::record;
    using F = datum_test::fallible_projection_record;
    using M = datum_test::move_projection_record;

    static_assert(std::is_nothrow_constructible_v<datum::view_of<R>, const R&>);
    static_assert(!std::is_nothrow_constructible_v<datum::view_of<F>, const F&>);
    static_assert(std::is_nothrow_constructible_v<datum::view_of<M>, const M&>);
    const F failing_accessor {{7, true}};
    CHECK_THROWS_AS(datum::view_of {failing_accessor}, std::runtime_error);
    const M valid {{7, true}};
    datum::view_of view {valid};
    CHECK(view.value.value == 7);
    // Explicitly moving an existing view still moves its chosen representation.
    CHECK_THROWS_AS(datum::view_of<M>(std::move(view)), std::runtime_error);
    struct source final
    {
        datum_test::immovable_record value;
    };
    const source value {{{9, false}}};
    const datum::view_of immovable {value};
    static_assert(!std::is_move_constructible_v<decltype(immovable)>);
    CHECK(immovable.value.value == 9);
}

#ifdef DATUM_HAVE_ZPP
 template <typename T>
 concept accepts_candidate_decode = requires(datum::binary_view bytes) {
     datum::decode_bounded<T, 128>(bytes, 128);
 };
 struct no_default_candidate
 {
     no_default_candidate() = delete;

 };
 static_assert(!accepts_candidate_decode<no_default_candidate>);
 static_assert(!accepts_candidate_decode<const datum_test::record>);
 static_assert(accepts_candidate_decode<datum_test::record>);
#endif

static_assert(datum::owning_value<datum_test::record>);
static_assert(!datum::owning_value<datum_test::borrowed_record>);
static_assert(datum::render::displayable<datum_test::record>);
#ifdef DATUM_HAVE_ZPP
 static_assert(datum::serializable<datum_test::record>);
#endif
static_assert(datum::render::displayable<datum::traversal::page_traversal_result<int>>);
#ifdef DATUM_HAVE_ZPP
 static_assert(!datum::serializable<datum::traversal::page_traversal_result<int>>);

 static_assert([] {
     datum_test::record value {"constant data", 41};
     auto bytes = datum::to_bytes(value);
     if (datum::from_bytes<datum_test::record>(bytes) != value) {
         return false;
     }
     datum_test::record restored;
     datum::to_bytes(value, bytes);
     datum::from_bytes(bytes, restored);
     std::string text;
     datum::render::format_to(std::back_inserter(text), restored);

     return restored == value and text == "record{name=\"constant data\", version=41}";
 }());

 static_assert([] {
     const datum_test::clock_record value {{-123}};
     const auto bytes = datum::to_bytes(value);

     return bytes == datum::to_bytes(std::int64_t {-123}) and
            datum::from_bytes<datum_test::clock_record>(bytes).value.count == -123;
 }());
#endif

static_assert([] {
    using schema = datum::render::enum_schema<datum_test::label>;

    return schema::name(datum_test::label::alias) == "first" and
           std::empty(schema::name(static_cast<datum_test::label>(99)));
}());

static_assert([] {
    std::string text;
    datum::render::format_to(std::back_inserter(text), datum_test::flag {});
    text += ';';
    datum::render::format_to(std::back_inserter(text), static_cast<datum_test::letter>(2));

    return text == "unknown(0);unknown(2)";
}());

static_assert([] {
    std::string text;
    for (const auto value : std::array {std::numeric_limits<std::int64_t>::min(), std::int64_t {0},
                                        std::numeric_limits<std::int64_t>::max()}) {
        datum::render_detail::append_integer(std::back_inserter(text), value);
        text += ';';
    }
    datum::render_detail::append_integer(std::back_inserter(text), std::numeric_limits<std::uint64_t>::max());

    return text == "-9223372036854775808;0;9223372036854775807;18446744073709551615";
}());

TEST_CASE("Datum reflected enum tables preserve aliases and unknown values", "[datum][data]")
{
    using schema = datum::render::enum_schema<datum_test::label>;

    CHECK(schema::name(datum_test::label::first) == "first");
    CHECK(schema::name(datum_test::label::alias) == "first");
    CHECK(schema::name(datum_test::label::last) == "last");
    CHECK(std::empty(schema::name(static_cast<datum_test::label>(99))));
}

TEST_CASE("Datum page progress is diagnostic data", "[datum][data]")
{
    const datum::traversal::page_traversal_result<int> progress {
        .resume = 8, .pages = 3, .outcome = datum::traversal::page_traversal_outcome::stopped};
    std::string text;
    datum::render::format_to(std::back_inserter(text), progress);
    CHECK(text == "page_traversal_result{resume=8, pages=3, outcome=stopped}");
}

TEST_CASE("Datum consumer schemas display and round-trip unrelated owning records", "[datum][data]")
{
    const datum_test::record value {"unrelated domain", 41};
    std::string text;
    datum::render::format_to(std::back_inserter(text), value);
    CHECK(text == "record{name=\"unrelated domain\", version=41}");
#ifdef DATUM_HAVE_ZPP
     const auto bytes = datum::to_bytes(value);
     CHECK(datum::from_bytes<datum_test::record>(bytes) == value);
     auto trailing = bytes;
     ::ceph::util::push_back(trailing, 0);
     CHECK_THROWS_AS(datum::from_bytes<datum_test::record>(trailing), std::runtime_error);
     CHECK_THROWS_AS(
         datum::from_bytes<datum_test::record>(datum::binary_view {bytes}.first(1)),
         std::runtime_error);
#endif
}

#ifdef DATUM_HAVE_ZPP
 TEST_CASE("codec failures retain structured errors and complete-input checks", "[datum][codec]")
 {
     const auto check_error = [](auto&& operation, const std::errc expected) {
         bool caught = false;
         try
          {
             operation();
          }
         catch (const std::system_error& error)
          {
             caught = true;
             CHECK(error.code() == std::make_error_code(expected));
          }
         CHECK(caught);
     };
     const datum_test::record source {"structured errors", 7};
     const auto bytes = datum::to_bytes(source);
     const auto incomplete = std::span {bytes}.first(1);
     check_error([&] { datum::from_bytes<datum_test::record>(incomplete); }, std::errc::result_out_of_range);
     check_error([&] { datum::decode_bounded<datum_test::record, 4>(bytes, bytes.size()); }, std::errc::message_size);
     std::array<std::uint8_t, 1> storage;
     check_error([&] { datum::to_bytes(source, std::span {storage}); }, std::errc::result_out_of_range);
     auto trailing = bytes;
     trailing.push_back(0);
     check_error([&] { datum::from_bytes<datum_test::record>(trailing); }, std::errc::bad_message);
     check_error([&] { datum::decode_bounded<datum_test::record, 128>(trailing, 512); }, std::errc::bad_message);

     struct custom_record final { datum_test::failing_field field; };
     using wire = datum::binary_fields<&custom_record::field>;
     datum::binary_buffer output;
     check_error([&] { datum::to_bytes<wire>(custom_record {}, output); }, std::errc::permission_denied);
     check_error([&] { datum::from_bytes<wire>(bytes); }, std::errc::permission_denied);
 }

 TEST_CASE("Datum field representations are selected by consumer profiles", "[datum][codec]")
 {
     const datum_test::clock_record value {{123}};
     const auto bytes = datum::to_bytes(value);
     CHECK(datum::from_bytes<datum_test::clock_record>(bytes).value.count == 123);
     CHECK(bytes == datum::to_bytes(std::int64_t {123}));
 }
#endif

TEST_CASE("generated member views preserve names and reject temporary records", "[datum][view]")
{
    datum_test::record source {"borrowed", 42};
    datum::view_of<datum_test::record> view {source};
    static_assert(std::same_as<decltype(view.name), const std::string&>);
    static_assert(!std::constructible_from<decltype(view), datum_test::record&&>);
    static_assert(!datum::owning_value<decltype(view)>);
    CHECK(&view.name == &source.name);
    CHECK(&view.version == &source.version);
    source.version = 43;
    CHECK(view.version == 43);
    struct derived_view : datum::view_of<datum_test::record>
    {
        using datum::view_of<datum_test::record>::view_of;
    };
    CHECK(derived_view {source}.version == 43);
    datum_test::collision_record collisions {1, 2};
    const datum::view_of named {collisions};
    CHECK(named.project == 1);
    CHECK(named.record_type == 2);
}

static_assert([] {
    datum_test::constructed_record source {7};
    const datum::view_of view {source};
    static_assert(std::same_as<decltype(view.value), const int&>);
    static_assert(!std::constructible_from<decltype(view), datum_test::constructed_record&&>);
    static_assert(!std::constructible_from<decltype(view), const datum_test::constructed_record&&>);
    static_assert(std::size(datum::detail::record_members<datum_test::constructed_record>) == 1);

    return &view.value == &source.value and view.value == 7;
}());

TEST_CASE("generated views observe constructor-bearing public records", "[datum][view]")
{
    datum_test::constructed_record source {41};
    struct constructed_view : datum::view_of<datum_test::constructed_record>
    {
        using datum::view_of<datum_test::constructed_record>::view_of;
    };
    const constructed_view view {source};
    CHECK(&view.value == &source.value);
    source.value = 42;
    CHECK(view.value == 42);
}

#ifdef DATUM_HAVE_ZPP
 TEST_CASE("fixed and fragmented codec endpoints share the enrolled wire", "[datum][bytes]")
 {
     const datum_test::record source {std::string(104, 'x'), 42};
     const auto expected = datum::to_bytes(source);
     std::array<std::uint8_t, 512> storage;
     storage.fill(0xAA);
     const auto written = datum::to_bytes(source, std::span {storage});
     CHECK(std::ranges::equal(written, expected));
     CHECK(storage[std::size(written)] == 0xAA);
     std::array<std::byte, 512> byte_storage {};
     const auto byte_output = datum::to_bytes(source, std::span {byte_storage});
     CHECK(datum::from_bytes<datum_test::record>(byte_output) == source);
     CHECK((datum::decode_bounded<datum_test::record, 128>(byte_output, 512) == source));
     std::array<std::uint8_t, 512> fragments_storage {};
     const std::array fragments {
         std::span {fragments_storage}.first(3), std::span {fragments_storage}.subspan(3, 9),
         std::span {fragments_storage}.subspan(12, std::size(expected) - 12)};
     CHECK(datum::to_bytes(source, fragments, std::span {storage}) == std::size(expected));
     CHECK(datum::from_bytes<datum_test::record>(fragments, std::span {storage}) == source);
     CHECK((datum::decode_bounded<datum_test::record, 128>(expected, 512) == source));
     CHECK_THROWS_AS((datum::decode_bounded<datum_test::record, 16>(expected, 512)), std::runtime_error);
     CHECK_THROWS_AS((datum::decode_bounded<datum_test::record, 128>(expected, 16)), std::length_error);
     CHECK_THROWS_AS(datum::to_bytes(source, std::span {storage}.first(8)), std::runtime_error);
     auto trailing = expected;
     ::ceph::util::push_back(trailing, 0);
     CHECK_THROWS_AS((datum::decode_bounded<datum_test::record, 128>(trailing, 512)), std::runtime_error);
     auto truncated = expected;
     truncated.pop_back();
     CHECK_THROWS_AS((datum::decode_bounded<datum_test::record, 128>(truncated, 512)), std::runtime_error);
     const std::array too_small {std::span {fragments_storage}.first(1)};
     fragments_storage.fill(0xAA);
     CHECK_THROWS_AS(datum::scatter_bytes(std::span {expected}, too_small), std::length_error);
     CHECK(fragments_storage.front() == 0xAA);
 }

 static_assert([] {
     datum_test::record source {"constexpr", 7};
     std::array<std::uint8_t, 64> storage {};
     const auto bytes = datum::to_bytes(source, std::span {storage});
     const datum::view_of view {source};

     return datum::decode_bounded<datum_test::record, 64>(bytes, 64) == source and
            view.version == 7;
 }());

 TEST_CASE("direct binary schemas choose declaration or explicit wire order", "[datum][schema]")
 {
     using wire = datum::binary_schema_t<datum_test::entry>;
     using reordered = datum::binary_fields<&datum_test::entry::value, &datum_test::entry::version,
                                            &datum_test::entry::key>;

     static_assert(std::same_as<reordered::record_type, datum_test::entry>);
     static_assert(std::same_as<reordered, datum::binary_schema_t<datum_test::entry, &datum_test::entry::value,
                                                      &datum_test::entry::version,
                                                      &datum_test::entry::key>>);

     static_assert(!datum::serializable<datum_test::entry>);
     const datum_test::entry source {"object/42", 7, "contents"};
     datum::binary_buffer expected;
     zpp::bits::out output {expected};
     output(source.key, source.version, source.value).or_throw();
     const auto bytes = datum::to_bytes<wire>(source);
     CHECK(bytes == expected);
     CHECK(datum::from_bytes<wire>(bytes) == source);

     ::ceph::util::clear(expected);
     output.reset();
     output(source.value, source.version, source.key).or_throw();
     const auto custom = datum::to_bytes<reordered>(source);
     CHECK(custom == expected);
     CHECK(datum::from_bytes<reordered>(custom) == source);

     using scalar_wire = datum::binary_schema_t<datum_test::scalar_record>;

     const datum_test::scalar_record scalar {3, 7};
     ::ceph::util::clear(expected);
     output.reset();
     output(scalar.first, scalar.second).or_throw();
     CHECK(datum::to_bytes<scalar_wire>(scalar) == expected);
     CHECK(datum::to_bytes(scalar) != expected);

     using clock_wire = datum::binary_fields<&datum_test::clock_record::value>;

     const datum_test::clock_record clock {{-123}};
     const auto clock_bytes = datum::to_bytes<clock_wire>(clock);
     CHECK(clock_bytes == datum::to_bytes(clock));
     CHECK(datum::from_bytes<clock_wire>(clock_bytes).value.count == -123);
 }

 TEST_CASE("selected binary schemas omit local fields and preserve their defaults",
           "[datum][schema]")
 {
     using wire = datum::binary_fields<&datum_test::local_entry::value, &datum_test::local_entry::key>;

     unsigned calls = 0;
     const datum_test::local_entry source {"key", 7, "payload", [&calls] { ++calls; }};
     datum::binary_buffer expected;
     zpp::bits::out output {expected};
     output(source.value, source.key).or_throw();
     const auto bytes = datum::to_bytes<wire>(source);
     CHECK(bytes == expected);
     const auto candidate = datum::from_bytes<wire>(bytes);
     CHECK(candidate.key == source.key);
     CHECK(candidate.value == source.value);
     CHECK(candidate.version == 99);
     CHECK_FALSE(candidate.on_change);
     CHECK(calls == 0);

     datum_test::local_entry target {"old", 42, "old", [&calls] { ++calls; }};
     datum::from_bytes<wire>(bytes, target);
     CHECK(target.key == source.key);
     CHECK(target.value == source.value);
     CHECK(target.version == 42);
     target.on_change();
     CHECK(calls == 1);
 }

 TEST_CASE("direct binary schemas share storage adapters and decode limits", "[datum][schema]")
 {
     using wire = datum::binary_fields<&datum_test::entry::key, &datum_test::entry::version,
                                      &datum_test::entry::value>;

     const datum_test::entry source {"key", 7, std::string(104, 'x')};
     const auto expected = datum::to_bytes<wire>(source);
     std::array<std::byte, 512> storage {};
     const auto written = datum::to_bytes<wire>(source, std::span {storage});
     CHECK(std::ranges::equal(written, expected, {},
                              [](std::byte value) { return std::to_integer<std::uint8_t>(value); }));
     CHECK(datum::from_bytes<wire>(written) == source);
     datum::binary_buffer reusable {1, 2, 3};
     datum::to_bytes<wire>(source, reusable);
     CHECK(reusable == expected);

     std::array<std::uint8_t, 512> staging {};
     std::array<std::uint8_t, 512> scattered {};
     const std::array fragments {std::span {scattered}.first(3),
                                 std::span {scattered}.subspan(3, std::size(expected) - 3)};
     CHECK(datum::to_bytes<wire>(source, fragments, std::span {staging}) ==
           std::size(expected));
     CHECK(datum::from_bytes<wire>(fragments, std::span {staging}) == source);
     CHECK((datum::decode_bounded<wire, 128>(expected, 512) == source));
     CHECK_THROWS_AS((datum::decode_bounded<wire, 16>(expected, 512)), std::runtime_error);
     CHECK_THROWS_AS((datum::decode_bounded<wire, 128>(expected, 1)), std::length_error);
     CHECK_THROWS_AS(datum::to_bytes<wire>(source, std::span {staging}.first(8)), std::runtime_error);
     auto damaged = expected;
     ::ceph::util::push_back(damaged, 0);
     CHECK_THROWS_AS(datum::from_bytes<wire>(damaged), std::runtime_error);
     damaged = expected;
     damaged.pop_back();
     CHECK_THROWS_AS(datum::from_bytes<wire>(damaged), std::runtime_error);
 }

 static_assert([] {
     using wire = datum::binary_fields<&datum_test::entry::key, &datum_test::entry::version,
                                      &datum_test::entry::value>;

     datum_test::entry source {"constexpr", 7, "payload"};
     std::array<std::uint8_t, 64> storage {};
     const auto bytes = datum::to_bytes<wire>(source, std::span {storage});

     return datum::decode_bounded<wire, 64>(bytes, 64) == source;
 }());

 TEST_CASE("schema inference adds no construction or ownership requirements", "[datum][schema]")
 {
     struct existing_record final
     {
         int selected;
         int local = 17;

     public:
         existing_record() = delete;
         explicit existing_record(const int value) : selected(value)
         {}
         existing_record(const existing_record&) = delete;
         existing_record(existing_record&&) = delete;

     };
     using wire = datum::binary_fields<&existing_record::selected>;

     static_assert(datum::binary_schema_type<wire>);
     static_assert(!datum::owning_value<existing_record>);
     static_assert(!datum::candidate_value<existing_record>);
     const existing_record source {42};
     const auto bytes = datum::to_bytes<wire>(source);
     existing_record target {0};

     datum::from_bytes<wire>(bytes, target);
     CHECK(target.selected == 42);
     CHECK(target.local == 17);

     // Native wire profiles may deliberately repeat a field; schema use may not.
     using repeated = datum::binary_fields<&existing_record::selected, &existing_record::selected>;

     static_assert(!datum::binary_schema_type<repeated>);
     datum::binary_buffer observed;
     zpp::bits::out output {observed};
     datum::serialize_schema(output, source, repeated {}).or_throw();
     auto expected = bytes;
     ::ceph::util::append_range(expected, bytes);
     CHECK(observed == expected);
 }

 TEST_CASE("empty records retain explicit binary schema identity", "[datum][schema]")
 {
     struct empty final
     {};
     using wire = datum::binary_schema_t<empty>;

     static_assert(datum::binary_schema_type<wire>);
     const auto bytes = datum::to_bytes<wire>(empty {});
     CHECK(std::empty(bytes));
     CHECK_NOTHROW(datum::from_bytes<wire>(bytes));
 }

#endif

#if defined(DATUM_DATA_NEGATIVE)
 void invalid_data_boundary()
 {
 #if DATUM_DATA_NEGATIVE == 1
      datum::view_of<datum_test::record> view {datum_test::record {"temporary", 1}};
      (void)view;
 #elif DATUM_DATA_NEGATIVE == 2
      struct bits
      {
          unsigned field : 1;
      };
      bits source {};
      datum::view_of<bits> view {source};
      (void)view;
 #elif DATUM_DATA_NEGATIVE == 3
      const std::array<std::uint8_t, 64> storage {};
  #ifdef DATUM_HAVE_ZPP
       datum::to_bytes(datum_test::record {}, std::span {storage});
  #endif
 #elif DATUM_DATA_NEGATIVE == 4
      std::array<signed char, 64> storage {};
  #ifdef DATUM_HAVE_ZPP
       datum::to_bytes(datum_test::record {}, std::span {storage});
  #endif
 #elif DATUM_DATA_NEGATIVE == 5
      struct base
      {};
      class derived : private base
      {

      public:
          int value;
      };
      derived source {};
      datum::view_of<derived> view {source};
      (void)view;
 #elif DATUM_DATA_NEGATIVE == 6
      using schema =
          datum::binary_schema_t<datum_test::entry, &datum_test::entry::key, &datum_test::entry::key>;
      (void)sizeof(schema);
 #elif DATUM_DATA_NEGATIVE == 7
      using schema = datum::binary_schema_t<datum_test::entry, &datum_test::scalar_record::first>;

      (void)sizeof(schema);
 #elif DATUM_DATA_NEGATIVE == 8
      struct borrowed
      {
          int& value;
      };
      using schema = datum::binary_schema_t<borrowed>;

      (void)sizeof(schema);
 #elif DATUM_DATA_NEGATIVE == 9
      using schema = datum::binary_schema_t<datum_test::entry>;

  #ifdef DATUM_HAVE_ZPP
       datum::to_bytes<schema>(datum_test::scalar_record {});
  #endif
 #elif DATUM_DATA_NEGATIVE == 10
      struct base
      {
          int inherited;
      };
      class derived : private base
      {

      public:
          int selected;
      };
      using schema = datum::binary_schema_t<derived>;

      (void)sizeof(schema);
 #endif
 }
#endif
