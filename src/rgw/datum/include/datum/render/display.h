// Datum: Enrolled diagnostic fields, projections and bounded character sinks.
// Generic observation machinery; consumers supply schemas and redaction.
//
// Please keep this layer independent of native records, hydration and storage or wire
// policy.
// Put domain-specific behavior in consumer profiles or adapters.

// -*- mode:C++; tab-width:8; c-basic-offset:4; indent-tabs-mode:nil -*-
// vim: ts=8 sw=4 sts=4 expandtab ft=cpp

#ifndef CEPH_RGW_DATUM_RENDER_DISPLAY_H
 #define CEPH_RGW_DATUM_RENDER_DISPLAY_H

#include "datum/bytes.h"
#include "datum/detail/record.h"

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <meta>
#include <type_traits>

#include <optional>
#include <system_error>
#include <tuple>
#include <utility>
#include <variant>

#include <array>

#include <algorithm>
#include <functional>
#include <iterator>
#include <ranges>

#include <charconv>
#include <format>
#include <string>
#include <string_view>

#include <chrono>
#include <ratio>

namespace ceph::salad::datum::render {

struct display_limits
{
    std::size_t bytes = 4096;

    std::size_t depth = 16;
};
} // ceph::salad::datum::render

namespace ceph::salad::datum::render_detail {

inline constexpr std::size_t maximum_display_text_bytes = 160;
inline constexpr std::size_t maximum_display_binary_bytes = 32;
inline constexpr std::size_t maximum_display_range_elements = 16;

// A named alias keeps its label; a specialization uses the template declaration.
consteval std::string_view reflected_record_name(const std::meta::info record)
{
    return std::meta::identifier_of(
        std::meta::has_identifier(record) ? record : std::meta::template_of(record));
}

} // ceph::salad::datum::render_detail

namespace ceph::salad::datum::render {

enum struct display_field_policy : std::uint8_t { value, redacted };

// Accessors may be member pointers or stateless callables, so borrowed views
// with reference members use the same schema machinery as owning records:
template <auto Accessor, display_field_policy Policy>
struct display_field final
{
    std::string_view name = std::meta::identifier_of(detail::record_member<Accessor>());
};

template <auto Accessor>
requires std::is_member_object_pointer_v<decltype(Accessor)> && (nullptr != Accessor)
constexpr auto field() noexcept
{
    return display_field<Accessor, display_field_policy::value> {};
}

template <auto Accessor>
constexpr auto field(const std::string_view name) noexcept
{
    return display_field<Accessor, display_field_policy::value> {name};
}

template <typename T>
struct display_schema;

template <typename T>
struct enum_schema;

template <typename T>
struct display_projection;

template <typename EnumT>
requires std::is_enum_v<EnumT>
struct reflected_enum_schema
{
    using entry_type = std::pair<EnumT, std::string_view>;

    // Reflection builds ordinary data; runtime selection needs no meta values.
    static constexpr auto identifiers = [] consteval {
        static constexpr auto enumerators =
            std::define_static_array(std::meta::enumerators_of(^^EnumT));
        std::array<entry_type, std::size(enumerators)> entries {};
        auto next = std::begin(entries);
        template for (constexpr auto enumerator : enumerators) {
            *next++ = entry_type {[:enumerator:], std::meta::identifier_of(enumerator)};
        }

        return entries;
    }();

public:
    static constexpr std::string_view name(const EnumT value)
    {
        const auto found = std::ranges::find(identifiers, value, &entry_type::first);

        return found == std::end(identifiers) ? std::string_view {} : found->second;
    }

};

} // ceph::salad::datum::render

namespace ceph::salad::datum::render_detail {

template <std::meta::info Member, std::meta::info ...Redacted>
consteval auto reflected_field()
{
    static_assert(std::meta::has_identifier(Member),
                  "Use an explicit projection for anonymous diagnostic members");
    // Splicing also borrows reference members without an accessor wrapper:
    constexpr auto accessor = [](const auto& value)
        -> std::conditional_t<std::meta::is_bit_field(Member),
                              std::remove_cvref_t<decltype(value.[:Member:])>,
                              decltype((value.[:Member:]))> { return value.[:Member:]; };
    constexpr auto policy = ((Member == Redacted) || ...) ? render::display_field_policy::redacted
                                                          : render::display_field_policy::value;

    return render::display_field<accessor, policy> {std::meta::identifier_of(Member)};
}

} // ceph::salad::datum::render_detail

namespace ceph::salad::datum::render {

// Enrollment is explicit; a new field requires a fresh confidentiality review.
// Selected or renamed fields still use an explicit schema or projection:
template <std::meta::info Record, std::size_t ApprovedMembers, std::meta::info ...Redacted>
struct reflected_display_schema
{
    using record_type = [:Record:];

    static_assert(!std::is_union_v<record_type>,
                  "Use an explicit projection for active union members");

    static constexpr auto members = detail::record_members<record_type>;

    static_assert(ApprovedMembers == std::size(members),
                  "Review new diagnostic members and their redaction before enrollment");
    static_assert(std::empty(std::meta::bases_of(Record,
                                                 std::meta::access_context::unprivileged())),
                  "Inherited diagnostic state requires an explicit projection");
    static_assert((std::ranges::contains(members, Redacted) && ...),
                  "Redaction must name a member of the displayed record");

    static constexpr std::string_view name = render_detail::reflected_record_name(Record);
    static constexpr auto fields = []<std::size_t ...Index>(std::index_sequence<Index...>) {
        return std::tuple {render_detail::reflected_field<members[Index], Redacted...>()...};
    }(std::make_index_sequence<std::size(members)> {});
};

} // ceph::salad::datum::render

namespace ceph::salad::datum::render_detail {

template <typename T>
concept display_record = requires { render::display_schema<std::remove_cvref_t<T>>::fields; };

template <display_record ValueT>
constexpr std::string_view record_name()
{
    using value_type = std::remove_cvref_t<ValueT>;

    if constexpr (requires { render::display_schema<value_type>::name; }) {
        return render::display_schema<value_type>::name;
    }

    if constexpr (!(requires { render::display_schema<value_type>::name; })) {
        return reflected_record_name(std::meta::dealias(^^value_type));
    }
}

template <typename T>
concept display_enum =
    std::is_enum_v<std::remove_cvref_t<T>> && requires(const std::remove_cvref_t<T> value) {
        {
            render::enum_schema<std::remove_cvref_t<T>>::name(value)
        } -> std::convertible_to<std::string_view>;
    };

template <typename T>
concept projected_display_value = requires(const std::remove_cvref_t<T>& value) {
    render::display_projection<std::remove_cvref_t<T>>::get(value);
};

// Reflection identifies exact standard specializations without trait scaffolding.
template <typename T>
consteval bool specialization_of(const std::meta::info primary)
{
    using value_type = std::remove_cvref_t<T>;

    constexpr auto reflected = std::meta::dealias(^^value_type);

    return std::meta::has_template_arguments(reflected) &&
           primary == std::meta::template_of(reflected);
}

template <typename T>
inline constexpr bool is_optional_v = specialization_of<T>(^^std::optional);

template <typename T>
concept duration_value = requires {
    typename std::remove_cvref_t<T>::rep;
    typename std::remove_cvref_t<T>::period;
    requires std::derived_from<std::remove_cvref_t<T>,
                               std::chrono::duration<typename std::remove_cvref_t<T>::rep,
                                                     typename std::remove_cvref_t<T>::period>>;
};

template <typename T>
inline constexpr bool is_time_point_v = specialization_of<T>(^^std::chrono::time_point);

template <typename T>
concept display_tuple = (specialization_of<T>(^^std::tuple) || specialization_of<T>(^^std::pair)) &&
                        !display_record<T> && !projected_display_value<T>;

template <typename T>
inline constexpr bool is_variant_v = specialization_of<T>(^^std::variant);

template <typename T>
concept text_value = std::convertible_to<const T&, std::string_view>;

template <typename T>
concept byte_range = std::ranges::input_range<const std::remove_reference_t<T>> &&
                     std::same_as<std::ranges::range_value_t<T>, std::byte>;

template <typename T>
concept display_range =
    std::ranges::input_range<const std::remove_reference_t<T>> && !projected_display_value<T> &&
    !is_optional_v<T> && !text_value<T> && !byte_range<T>;

template <typename OutputT>
concept character_output =
    std::move_constructible<OutputT> && std::assignable_from<OutputT&, OutputT> &&
    requires (OutputT output, const char value) { *output++ = value; };

struct display_state
{
    render::display_limits limits;

    std::size_t written = 0;
    std::size_t depth = 0;
    bool truncated = false;
};

template <character_output OutputT>
struct bounded_output
{
    OutputT output;
    display_state *state;

public:
    constexpr bounded_output& operator*() noexcept { return *this; }

    constexpr bounded_output& operator++(int) noexcept { return *this; }
    constexpr void operator=(char value)
    {
        if (state->written == state->limits.bytes) {
            state->truncated = true;

            return;
        }
        *output++ = value;
        ++state->written;
    }

};

template <character_output OutputT>
constexpr bool output_full(const OutputT&) noexcept
{
    return false;
}

template <character_output OutputT>
constexpr bool output_full(const bounded_output<OutputT>& output) noexcept
{
    return output.state->written == output.state->limits.bytes;
}

template <character_output OutputT>
struct depth_scope
{
    bool entered = true;

public:
    constexpr explicit depth_scope(const OutputT&) noexcept {}

};

template <character_output OutputT>
struct depth_scope<bounded_output<OutputT>>
{
    display_state *state;
    bool entered;

public:
    constexpr explicit depth_scope(const bounded_output<OutputT>& output) noexcept
        : state(output.state), entered(state->depth < state->limits.depth && !output_full(output))
    {
        if (!entered) {
            state->truncated = true;

            return;
        }
        ++state->depth;
    }

public:
    constexpr ~depth_scope()
    {
        if (entered) {
            --state->depth;
        }
    }

};
template <character_output OutputT>
constexpr OutputT append_text(OutputT output, std::string_view text);

template <character_output OutputT, std::integral IntegerT>
requires (!std::same_as<std::remove_cv_t<IntegerT>, bool> &&
         !std::same_as<std::remove_cv_t<IntegerT>, char>)
constexpr OutputT append_integer(OutputT output, IntegerT value);

template <character_output OutputT, typename ValueT>
constexpr OutputT append_value(OutputT output, const ValueT& value);

template <character_output OutputT, typename ValueT>
requires std::ranges::sized_range<const ValueT>
constexpr OutputT append_redacted(OutputT output, const ValueT& value)
{
    output = append_text(std::move(output), "<redacted:");
    output = append_integer(std::move(output), std::ranges::size(value));
    constexpr auto units =
        byte_value<std::ranges::range_value_t<const ValueT>> ? " bytes>" : " elements>";

    return append_text(std::move(output), units);
}

template <character_output OutputT, typename ValueT>
requires (!std::ranges::sized_range<const ValueT>)
constexpr OutputT append_redacted(OutputT output, const ValueT&)
{
    return append_text(std::move(output), "<redacted>");
}

template <character_output OutputT, typename ValueT, auto Accessor>
constexpr OutputT
append_field_value(OutputT output, const ValueT& value,
                   const render::display_field<Accessor, render::display_field_policy::value>)
{
    return append_value(std::move(output), std::invoke(Accessor, value));
}

template <character_output OutputT, typename ValueT, auto Accessor>
constexpr OutputT
append_field_value(OutputT output, const ValueT& value,
                   const render::display_field<Accessor, render::display_field_policy::redacted>)
{
    using field_type =
        std::remove_reference_t<std::invoke_result_t<decltype(Accessor), const ValueT&>>;
    if constexpr (std::ranges::sized_range<const field_type>) {
        return append_redacted(std::move(output), std::invoke(Accessor, value));
    }
    // No size is displayed; the field need not be observed to redact it.
    return append_text(std::move(output), "<redacted>");
}

template <character_output OutputT>
constexpr OutputT append_text(OutputT output, const std::string_view text)
{
    if constexpr (std::output_iterator<OutputT, char>) {
        return std::ranges::copy(text, std::move(output)).out;
    }
    for (const char value : text) {
        *output++ = value;
    }

    return output;
}

template <character_output OutputT>
constexpr OutputT append_hex_byte(OutputT output, const std::byte value)
{
    constexpr std::string_view digits = "0123456789ABCDEF";
    const auto byte = std::to_integer<unsigned char>(value);

    *output++ = digits[byte >> 4];
    *output++ = digits[byte & 0x0F];

    return output;
}

template <character_output OutputT>
constexpr OutputT append_escaped_byte(OutputT output, const unsigned char value)
{
    switch (value) {
    case '\\':
        return append_text(std::move(output), "\\\\");
    case '"':
        return append_text(std::move(output), "\\\"");
    case '\n':
        return append_text(std::move(output), "\\n");
    case '\r':
        return append_text(std::move(output), "\\r");
    case '\t':
        return append_text(std::move(output), "\\t");
    }

    if (0x20 <= value && 0x7F > value) {
        *output++ = static_cast<char>(value);

        return output;
    }

    output = append_text(std::move(output), "\\x");

    return append_hex_byte(std::move(output), std::byte {value});
}

template <character_output OutputT>
constexpr OutputT append_quoted(OutputT output, const std::string_view value)
{
    *output++ = '"';

    const auto displayed = std::min(std::size(value), maximum_display_text_bytes);

    for (const auto byte : value.substr(0, displayed)) {
        output = append_escaped_byte(std::move(output), static_cast<unsigned char>(byte));
    }

    if (displayed < std::size(value)) {
        output = append_text(std::move(output), "...");
    }

    *output++ = '"';

    return output;
}

template <character_output OutputT, std::integral IntegerT>
requires (!std::same_as<std::remove_cv_t<IntegerT>, bool> &&
         !std::same_as<std::remove_cv_t<IntegerT>, char>)
constexpr OutputT append_integer(OutputT output, const IntegerT value)
{
    // The buffer covers every decimal digit and a possible minus sign:
    std::array<char, std::numeric_limits<IntegerT>::digits10 + 3> buffer;
    const auto [position, error] =
        std::to_chars(std::data(buffer), std::data(buffer) + std::size(buffer), value);
    [[assume(std::errc {} == error)]];

    return append_text(std::move(output), {std::data(buffer), position});
}

template <character_output OutputT, display_enum EnumT>
constexpr OutputT append_enum(OutputT output, const EnumT value)
{
    const auto name = render::enum_schema<std::remove_cvref_t<EnumT>>::name(value);

    if (!std::empty(name)) {
        return append_text(std::move(output), name);
    }

    output = append_text(std::move(output), "unknown(");
    // Integral promotion also lets char and bool underlying types print as numbers:
    output = append_integer(std::move(output), +std::to_underlying(value));
    *output++ = ')';

    return output;
}

template <character_output OutputT, display_record ValueT>
constexpr OutputT append_record(OutputT output, const ValueT& value)
{
    depth_scope scope {output};
    if (!scope.entered) {
        return output;
    }
    using value_type = std::remove_cvref_t<ValueT>;

    output = append_text(std::move(output), record_name<value_type>());
    *output++ = '{';
    bool first = true;

    template for (constexpr auto descriptor : render::display_schema<value_type>::fields) {
        if (!output_full(output)) {
            if (!first) {
                output = append_text(std::move(output), ", ");
            }

            first = false;
            output = append_text(std::move(output), descriptor.name);
            *output++ = '=';
            if (!output_full(output)) {
                output = append_field_value(std::move(output), value, descriptor);
            }
        }
    }

    *output++ = '}';

    return output;
}

template <character_output OutputT, byte_range RangeT>
constexpr OutputT append_binary(OutputT output, const RangeT& value)
{
    output = append_text(std::move(output), "hex\"");
    auto current = std::ranges::begin(value);

    const auto end = std::ranges::end(value);
    std::size_t displayed = 0;
    for (; current != end && displayed < maximum_display_binary_bytes && !output_full(output);
         ++current, ++displayed) {
        output = append_hex_byte(std::move(output), *current);
    }
    if (current != end) {
        output = append_text(std::move(output), "...");
    }

    *output++ = '"';

    return output;
}

template <character_output OutputT, display_range RangeT>
constexpr OutputT append_range(OutputT output, const RangeT& value)
{
    depth_scope scope {output};
    if (!scope.entered) {
        return output;
    }
    *output++ = '[';
    auto current = std::ranges::begin(value);

    const auto end = std::ranges::end(value);
    std::size_t displayed = 0;
    for (; current != end && displayed < maximum_display_range_elements && !output_full(output);
         ++current, ++displayed) {
        if (displayed != 0) {
            output = append_text(std::move(output), ", ");
        }
        if (output_full(output)) {
            break;
        }
        output = append_value(std::move(output), *current);
    }
    if (current != end) {
        output = append_text(std::move(output), ", ...");
    }

    *output++ = ']';

    return output;
}

template <character_output OutputT, typename RepT, typename PeriodT>
constexpr OutputT append_duration(OutputT output, const std::chrono::duration<RepT, PeriodT>& value)
{
    // Preserve count and units; casting to a common duration can truncate/overflow.
    output = append_value(std::move(output), value.count());
    constexpr auto suffix = [] {
        if constexpr (std::ratio_equal_v<PeriodT, std::nano>) {
            return "ns";
        }

        if constexpr (std::ratio_equal_v<PeriodT, std::micro>) {
            return "us";
        }

        if constexpr (std::ratio_equal_v<PeriodT, std::milli>) {
            return "ms";
        }

        if constexpr (std::ratio_equal_v<PeriodT, std::ratio<1>>) {
            return "s";
        }

        if constexpr (std::ratio_equal_v<PeriodT, std::ratio<60>>) {
            return "min";
        }

        if constexpr (std::ratio_equal_v<PeriodT, std::ratio<3600>>) {
            return "h";
        }

        if constexpr (std::ratio_equal_v<PeriodT, std::ratio<86400>>) {
            return "d";
        }

        return "";
    }();
    if (std::size(std::string_view {suffix})) {
        return append_text(std::move(output), suffix);
    }
    *output++ = '[';
    output = append_integer(std::move(output), PeriodT::num);
    *output++ = '/';
    output = append_integer(std::move(output), PeriodT::den);

    return append_text(std::move(output), "]s");
}

template <character_output OutputT, display_tuple TupleT>
constexpr OutputT append_tuple(OutputT output, const TupleT& value)
{
    depth_scope scope {output};
    if (!scope.entered) {
        return output;
    }
    *output++ = '(';
    std::size_t displayed = 0;
    const auto& [...element] = value;
    auto emit = [&output, &displayed](const auto& item) {
        if (displayed == maximum_display_range_elements || output_full(output)) {
            return;
        }
        if (displayed) {
            output = append_text(std::move(output), ", ");
        }
        if (output_full(output)) {
            return;
        }
        output = append_value(std::move(output), item);
        ++displayed;
    };
    (emit(element), ...);
    if (displayed < std::tuple_size_v<TupleT>) {
        output = append_text(std::move(output), ", ...");
    }
    *output++ = ')';

    return output;
}

// Predicates are deferred: later recognition may inspect an ill-formed interface.
// The first match preserves explicit enrollment and the existing value priority.
enum struct display_kind {
    record,
    projection,
    enumeration,
    text,
    boolean,
    character,
    integer,
    floating,
    duration,
    time_point,
    tuple,
    optional,
    variant,
    monostate,
    binary,
    range,
    unsupported
};

template <display_kind Kind, auto Predicate>
struct display_rule
{
    static constexpr auto kind = Kind;
    static constexpr auto matches = Predicate;
};

using display_categories = std::tuple<
    display_rule<display_kind::record, []<typename T>() { return display_record<T>; }>,
    display_rule<display_kind::projection, []<typename T>() { return projected_display_value<T>; }>,
    display_rule<display_kind::enumeration, []<typename T>() { return display_enum<T>; }>,
    display_rule<display_kind::text, []<typename T>() { return text_value<T>; }>,
    display_rule<display_kind::boolean,
                 []<typename T>() { return std::same_as<std::remove_cvref_t<T>, bool>; }>,
    display_rule<display_kind::character,
                 []<typename T>() { return std::same_as<std::remove_cvref_t<T>, char>; }>,
    display_rule<display_kind::integer,
                 []<typename T>() { return std::integral<std::remove_cvref_t<T>>; }>,
    display_rule<display_kind::floating,
                 []<typename T>() { return std::floating_point<std::remove_cvref_t<T>>; }>,
    display_rule<display_kind::duration, []<typename T>() { return duration_value<T>; }>,
    display_rule<display_kind::time_point, []<typename T>() { return is_time_point_v<T>; }>,
    display_rule<display_kind::tuple, []<typename T>() { return display_tuple<T>; }>,
    display_rule<display_kind::optional, []<typename T>() { return is_optional_v<T>; }>,
    display_rule<display_kind::variant, []<typename T>() { return is_variant_v<T>; }>,
    display_rule<display_kind::monostate,
                 []<typename T>() { return std::same_as<std::remove_cvref_t<T>, std::monostate>; }>,
    display_rule<display_kind::binary, []<typename T>() { return byte_range<T>; }>,
    display_rule<display_kind::range, []<typename T>() { return display_range<T>; }>>;

template <typename T, std::size_t Index = 0>
consteval display_kind display_category()
{
    if constexpr (Index == std::tuple_size_v<display_categories>) {
        return display_kind::unsupported;
    }

    if constexpr (Index < std::tuple_size_v<display_categories>) {
        using category = std::tuple_element_t<Index, display_categories>;

        constexpr bool matches = category::matches.template operator()<T>();
        if constexpr (matches) {
            return category::kind;
        }

        if constexpr (!matches) {
            return display_category<T, Index + 1>();
        }
    }
}

template <character_output OutputT, typename ValueT>
constexpr OutputT append_value(OutputT output, const ValueT& value)
{
    using value_type = std::remove_cvref_t<ValueT>;

    constexpr auto category = display_category<ValueT>();
    if constexpr (category == display_kind::record) {
        return append_record(std::move(output), value);
    }

    if constexpr (category == display_kind::projection) {
        depth_scope scope {output};
        if (!scope.entered) {
            return output;
        }

        return append_value(std::move(output), render::display_projection<value_type>::get(value));
    }

    if constexpr (category == display_kind::enumeration) {
        return append_enum(std::move(output), value);
    }

    if constexpr (category == display_kind::text) {
        return append_quoted(std::move(output), std::string_view {value});
    }

    if constexpr (category == display_kind::boolean) {
        return append_text(std::move(output), value ? "true" : "false");
    }

    if constexpr (category == display_kind::character) {
        return append_escaped_byte(std::move(output), static_cast<unsigned char>(value));
    }

    if constexpr (category == display_kind::integer) {
        return append_integer(std::move(output), value);
    }

    if constexpr (category == display_kind::floating) {
        std::array<char, 128> buffer;
        const auto [position, error] =
            std::to_chars(std::data(buffer), std::data(buffer) + std::size(buffer), value);
        if (std::errc {} != error) {
            throw std::system_error {std::make_error_code(error)};
        }

        return append_text(std::move(output), {std::data(buffer), position});
    }

    if constexpr (category == display_kind::duration) {
        return append_duration(std::move(output), value);
    }

    if constexpr (category == display_kind::time_point) {
        // Preserve the clock's count and units without an overflowing conversion.
        return append_value(std::move(output), value.time_since_epoch());
    }

    if constexpr (category == display_kind::tuple) {
        return append_tuple(std::move(output), value);
    }

    if constexpr (category == display_kind::optional) {
        depth_scope scope {output};
        if (!scope.entered) {
            return output;
        }

        return value ? append_value(std::move(output), *value)
                     : append_text(std::move(output), "null");
    }

    if constexpr (category == display_kind::variant) {
        depth_scope scope {output};
        if (!scope.entered) {
            return output;
        }
        if (value.valueless_by_exception()) [[unlikely]] {
            return append_text(std::move(output), "<valueless>");
        }
        std::visit(
            [&output](const auto& alternative) {
                output = append_value(std::move(output), alternative);
            },
            value);

        return output;
    }

    if constexpr (category == display_kind::monostate) {
        return append_text(std::move(output), "none");
    }

    if constexpr (category == display_kind::binary) {
        return append_binary(std::move(output), value);
    }

    if constexpr (category == display_kind::range) {
        return append_range(std::move(output), value);
    }

    if constexpr (category == display_kind::unsupported) {
        static_assert(false, "Enroll a diagnostic schema or projection for this value type");
    }
}

template <character_output OutputT, typename ValueT>
constexpr OutputT format_value(OutputT output, const ValueT& value,
                               render::display_limits limits = {})
{
    display_state state {limits};
    auto result = append_value(bounded_output {std::move(output), &state}, value);

    return std::move(result.output);
}

template <typename ValueT>
struct formatter
{
    constexpr auto parse(auto& context) { return context.begin(); }

    constexpr auto format(const ValueT& value, auto& context) const
    {
        return format_value(context.out(), value);
    }

};

} // ceph::salad::datum::render_detail

namespace ceph::salad::datum::render {

template <typename T>
concept displayable = render_detail::display_record<T> || render_detail::display_enum<T> ||
                      render_detail::projected_display_value<T>;

template <typename OutputT>
struct display_result
{
    OutputT output;

    std::size_t written;
    bool truncated;
};

// Bounded diagnostics are a prefix, not a wire format. Report truncation when
// callers need to distinguish a complete observation from its bounded prefix.
template <render_detail::character_output OutputT, displayable ValueT>
constexpr display_result<OutputT> format_bounded(OutputT output, const ValueT& value,
                                                 display_limits limits = {})
{
    render_detail::display_state state {limits};
    auto result = render_detail::append_value(
        render_detail::bounded_output {std::move(output), &state}, value);

    return {std::move(result.output), state.written, state.truncated};
}

template <render_detail::character_output OutputT, displayable ValueT>
constexpr OutputT format_to(OutputT output, const ValueT& value, display_limits limits = {})
{
    return render_detail::format_value(std::move(output), value, limits);
}

} // ceph::salad::datum::render

namespace std {

template <::ceph::salad::datum::render::displayable ValueT>
struct formatter<ValueT, char> : ::ceph::salad::datum::render_detail::formatter<ValueT>
{};

} // std

#endif // CEPH_RGW_DATUM_RENDER_DISPLAY_H
