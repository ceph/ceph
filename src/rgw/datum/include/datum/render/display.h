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
#include "datum/render/detail/schema.h"
#include "datum/render/detail/type_traits.h"
#include "datum/compat/charconv.h"

#if !defined(__cpp_structured_bindings) || __cpp_structured_bindings < 202411L
 #include "datum/compat/detail/render.h"
#endif

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
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

template <typename OutputT>
struct display_result
{
    OutputT output;

    std::size_t written;
    bool truncated;
};

} // ceph::salad::datum::render

namespace ceph::salad::datum::render_detail {

inline constexpr std::size_t maximum_display_text_bytes = 160;
inline constexpr std::size_t maximum_display_binary_bytes = 32;
inline constexpr std::size_t maximum_display_range_elements = 16;

template <typename T>
concept display_record = requires { render::display_schema<std::remove_cvref_t<T>>::fields; };

template <display_record ValueT>
constexpr std::string_view record_name()
{
    using value_type = std::remove_cvref_t<ValueT>;

    if constexpr (requires { render::display_schema<value_type>::name; }) {
        return render::display_schema<value_type>::name;
    }

    if constexpr (!requires { render::display_schema<value_type>::name; }) {
#if defined(__cpp_lib_reflection)
        return reflected_record_name(std::meta::dealias(^^value_type));
#else
        return compat::detail::implicit_record_name<value_type>();
#endif
    }
}

template <typename T>
concept display_enum =
    std::is_enum_v<std::remove_cvref_t<T>> and requires(const std::remove_cvref_t<T> value) {
        {
            render::enum_schema<std::remove_cvref_t<T>>::name(value)
        } -> std::convertible_to<std::string_view>;
    };

template <typename T>
concept projected_display_value = requires(const std::remove_cvref_t<T>& value) {
    render::display_projection<std::remove_cvref_t<T>>::get(value);
};

template <typename T>
concept duration_value = requires {
    typename std::remove_cvref_t<T>::rep;
    typename std::remove_cvref_t<T>::period;
    requires std::derived_from<std::remove_cvref_t<T>,
                               std::chrono::duration<typename std::remove_cvref_t<T>::rep,
                                                     typename std::remove_cvref_t<T>::period>>;
};

template <typename T>
concept text_value = std::convertible_to<const T&, std::string_view>;

template <typename T>
concept byte_range =
    std::ranges::input_range<const std::remove_reference_t<T>> and
    std::same_as<std::ranges::range_value_t<const std::remove_reference_t<T>>, std::byte>;

template <typename OutputT>
concept character_output =
    std::move_constructible<OutputT> and std::assignable_from<OutputT&, OutputT> and
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
        : state(output.state), entered(state->depth < state->limits.depth and !output_full(output))
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
requires (!std::same_as<std::remove_cv_t<IntegerT>, bool> and
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

    if (0x20 <= value and 0x7F > value) {
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
requires (!std::same_as<std::remove_cv_t<IntegerT>, bool> and
         !std::same_as<std::remove_cv_t<IntegerT>, char>)
constexpr OutputT append_integer(OutputT output, const IntegerT value)
{
    // The buffer covers every decimal digit and a possible minus sign:
    std::array<char, std::numeric_limits<IntegerT>::digits10 + 3> buffer;
    const auto [position, error] =
#if defined(__cpp_lib_constexpr_charconv) and __cpp_lib_constexpr_charconv >= 202207L
        std::to_chars(std::data(buffer), std::data(buffer) + std::size(buffer), value);
#else
        compat::to_decimal_chars(std::data(buffer), std::data(buffer) + std::size(buffer), value);
#endif
#if __has_cpp_attribute(assume) >= 202207L
    [[assume(std::errc {} == error)]];
#else
    static_cast<void>(error);
#endif

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

    [[maybe_unused]] auto emit = [&output, &first, &value](const auto& descriptor) {
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
    };
#if defined(__cpp_lib_reflection)
    template for (constexpr auto descriptor : render::display_schema<value_type>::fields) {
        emit(descriptor);
    }
#else
    compat::detail::visit_display_tuple(render::display_schema<value_type>::fields, emit);
#endif

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
    for (; current != end and displayed < maximum_display_binary_bytes and !output_full(output);
         ++current, ++displayed) {
        output = append_hex_byte(std::move(output), *current);
    }
    if (current != end) {
        output = append_text(std::move(output), "...");
    }

    *output++ = '"';

    return output;
}

template <character_output OutputT, typename RangeT>
requires std::ranges::input_range<const RangeT>
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
    for (; current != end and displayed < maximum_display_range_elements and !output_full(output);
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
#if defined(__cpp_structured_bindings) and __cpp_structured_bindings >= 202411L
    [[maybe_unused]] const auto& [...element] = value;
#endif
    [[maybe_unused]] auto emit = [&output, &displayed](const auto& item) {
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
#if defined(__cpp_structured_bindings) and __cpp_structured_bindings >= 202411L
    (emit(element), ...);
#else
    compat::detail::visit_display_tuple(value, emit);
#endif
    if (displayed < std::tuple_size_v<TupleT>) {
        output = append_text(std::move(output), ", ...");
    }
    *output++ = ')';

    return output;
}

template <character_output OutputT, typename ValueT>
constexpr OutputT append_builtin_value(OutputT& output, const ValueT& value)
{
    using value_type = std::remove_cvref_t<ValueT>;

    if constexpr (text_value<ValueT>) {
        return append_quoted(std::move(output), std::string_view {value});
    }

    if constexpr (!text_value<ValueT>) {
        if constexpr (std::same_as<value_type, bool>) {
            return append_text(std::move(output), value ? "true" : "false");
        }

        if constexpr (std::same_as<value_type, char>) {
            return append_escaped_byte(std::move(output), static_cast<unsigned char>(value));
        }

        if constexpr (std::integral<value_type> and !std::same_as<value_type, bool> and
                      !std::same_as<value_type, char>) {
            return append_integer(std::move(output), value);
        }

        if constexpr (std::floating_point<value_type>) {
            std::array<char, 128> buffer;
            const auto [position, error] =
                std::to_chars(std::data(buffer), std::data(buffer) + std::size(buffer), value);
            if (std::errc {} != error) {
                throw std::system_error {std::make_error_code(error)};
            }

            return append_text(std::move(output), {std::data(buffer), position});
        }

        if constexpr (duration_value<ValueT>) {
            return append_duration(std::move(output), value);
        }

        if constexpr (is_time_point_v<ValueT>) {
            // Preserve the clock's count and units without an overflowing conversion.
            return append_value(std::move(output), value.time_since_epoch());
        }

        if constexpr (display_tuple<ValueT>) {
            return append_tuple(std::move(output), value);
        }

        if constexpr (is_optional_v<ValueT>) {
            depth_scope scope {output};
            if (!scope.entered) {
                return output;
            }

            return value ? append_value(std::move(output), *value)
                         : append_text(std::move(output), "null");
        }

        if constexpr (is_variant_v<ValueT>) {
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

        if constexpr (std::same_as<value_type, std::monostate>) {
            return append_text(std::move(output), "none");
        }

        if constexpr (!std::integral<value_type> and
                      !std::floating_point<value_type> and !duration_value<ValueT> and
                      !is_time_point_v<ValueT> and !display_tuple<ValueT> and !is_optional_v<ValueT> and
                      !is_variant_v<ValueT> and !std::same_as<value_type, std::monostate>) {
            if constexpr (byte_range<ValueT>) {
                return append_binary(std::move(output), value);
            }

            if constexpr (!byte_range<ValueT> and std::ranges::input_range<const ValueT>) {
                return append_range(std::move(output), value);
            }

            if constexpr (!byte_range<ValueT> and !std::ranges::input_range<const ValueT>) {
#if defined(__cpp_lib_reflection)
                static_assert(false, "Enroll a diagnostic schema or projection for this value type");
#else
                static_assert(compat::detail::unsupported_display<ValueT>,
                              "Enroll a diagnostic schema or projection for this value type");
#endif
            }
        }
    }
}

// Enrolled records and projections shield unrelated, possibly ill-formed interfaces.
template <character_output OutputT, typename ValueT>
constexpr OutputT append_value(OutputT output, const ValueT& value)
{
    using value_type = std::remove_cvref_t<ValueT>;

    if constexpr (display_record<ValueT>) {
        return append_record(std::move(output), value);
    }

    if constexpr (!display_record<ValueT>) {
        if constexpr (projected_display_value<ValueT>) {
            depth_scope scope {output};
            if (!scope.entered) {
                return output;
            }

            return append_value(std::move(output), render::display_projection<value_type>::get(value));
        }

        if constexpr (!projected_display_value<ValueT>) {
            if constexpr (display_enum<ValueT>) {
                return append_enum(std::move(output), value);
            }

            if constexpr (!display_enum<ValueT>) {
                return append_builtin_value(output, value);
            }
        }
    }
}

template <character_output OutputT, typename ValueT>
constexpr render::display_result<OutputT> format_bounded(OutputT& output, const ValueT& value,
                                                         render::display_limits limits)
{
    render_detail::display_state state {limits};
    auto result = render_detail::append_value(
        render_detail::bounded_output<OutputT> {std::move(output), &state}, value);

    return {std::move(result.output), state.written, state.truncated};
}

template <character_output OutputT, typename ValueT>
constexpr OutputT format_value(OutputT output, const ValueT& value, render::display_limits limits = {})
{
    display_state state {limits};
    auto result = append_value(bounded_output<OutputT> {std::move(output), &state}, value);

    return std::move(result.output);
}

template <typename ValueT, typename ParseErrorT>
struct formatter
{
    constexpr auto parse(auto& context)
    {
        const auto position = context.begin();
        if (position != context.end() and *position != '}') {
            throw ParseErrorT {"Datum diagnostics accept only an empty format specification"};
        }

        return position;
    }

    constexpr auto format(const ValueT& value, auto& context) const
    {
        return ::ceph::salad::datum::render_detail::format_value(context.out(), value);
    }

};

} // ceph::salad::datum::render_detail

namespace ceph::salad::datum::render {

template <typename T>
concept displayable = render_detail::display_record<T> || render_detail::display_enum<T> ||
                      render_detail::projected_display_value<T>;

// A borrowed format argument explicitly selects Datum diagnostics and their limits.
// Use it when another formatting protocol also recognizes the source type.
template <displayable ValueT>
class display_view final
{
    const ValueT& observed;
    display_limits bounds;

public:
    constexpr explicit display_view(const ValueT& value, const display_limits limits = {}) noexcept
        : observed(value), bounds(limits)
    {}
    display_view(const ValueT&&, display_limits = {}) = delete;

public:
    constexpr const ValueT& value() const noexcept { return observed; }
    constexpr display_limits limits() const noexcept { return bounds; }

};

template <displayable ValueT>
constexpr auto display(const ValueT& value, const display_limits limits = {}) noexcept
{
    return display_view<ValueT> {value, limits};
}

template <displayable ValueT>
void display(const ValueT&&, display_limits = {}) = delete;

// Bounded diagnostics are a prefix, not a wire format. Report truncation when
// callers need to distinguish a complete observation from its bounded prefix.
template <render_detail::character_output OutputT, displayable ValueT>
constexpr display_result<OutputT> format_bounded(OutputT output, const ValueT& value, display_limits limits = {})
{
    return ::ceph::salad::datum::render_detail::format_bounded(output, value, limits);
}

template <render_detail::character_output OutputT, displayable ValueT>
constexpr OutputT format_to(OutputT output, const ValueT& value, display_limits limits = {})
{
    return render_detail::format_value(std::move(output), value, limits);
}

template <render_detail::character_output OutputT, displayable ValueT>
constexpr auto format_bounded(OutputT output, const display_view<ValueT>& value)
{
    return ::ceph::salad::datum::render_detail::format_bounded(
        output, value.value(), value.limits());
}

template <render_detail::character_output OutputT, displayable ValueT>
constexpr OutputT format_to(OutputT output, const display_view<ValueT>& value)
{
    return ::ceph::salad::datum::render_detail::format_value(
        std::move(output), value.value(), value.limits());
}

} // ceph::salad::datum::render

namespace ceph::salad::datum::render_detail {

template <render::displayable ValueT, typename ParseErrorT>
struct formatter<render::display_view<ValueT>, ParseErrorT> : formatter<ValueT, ParseErrorT>
{
    constexpr auto format(const render::display_view<ValueT>& value, auto& context) const
    {
        return ::ceph::salad::datum::render_detail::format_value(
            context.out(), value.value(), value.limits());
    }

};

} // ceph::salad::datum::render_detail

#endif // CEPH_RGW_DATUM_RENDER_DISPLAY_H
