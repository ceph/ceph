// Datum: Standard range insertion and controlled visitation.
// Use native C++23 members when present, with the same range-based caller interface
// on standard libraries that do not yet provide them.
#ifndef CEPH_RGW_DATUM_RANGE_H
 #define CEPH_RGW_DATUM_RANGE_H

#include <concepts>
#include <cstddef>
#include <expected>
#include <functional>
#include <optional>
#include <ranges>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>

namespace ceph::salad::datum {

enum struct acceptance { accept, accept_and_stop, reject_and_stop, skip };

struct visit_result final
{
    std::size_t offered = 0;
    std::size_t accepted = 0;
    // Present only for accept_and_stop or reject_and_stop.
    std::optional<acceptance> stop;

    constexpr bool stopped() const noexcept { return stop.has_value(); }

    constexpr std::size_t consumed() const noexcept
    {
        return offered - (stop == acceptance::reject_and_stop);
    }
};

template <typename ErrorT>
struct visit_failure final
{
    visit_result progress;
    ErrorT error;
};

namespace detail {

template <typename T>
struct result_traits
{
    using value_type = T;
    using error_type = void;
};

template <typename ValueT, typename ErrorT>
struct result_traits<std::expected<ValueT, ErrorT>>
{
    using value_type = ValueT;
    using error_type = ErrorT;
};

template <typename T>
constexpr T& result_value(T& result) noexcept
{
    return result;
}

template <typename ValueT, typename ErrorT>
constexpr decltype(auto) result_value(std::expected<ValueT, ErrorT>& result) noexcept
{
    return *result;
}

template <typename LeftT, typename RightT>
concept compatible_result_errors =
    std::is_void_v<typename result_traits<LeftT>::error_type> ||
    std::is_void_v<typename result_traits<RightT>::error_type> ||
    std::same_as<typename result_traits<LeftT>::error_type,
                 typename result_traits<RightT>::error_type>;

template <typename T>
concept acceptance_result = std::same_as<T, std::remove_cvref_t<T>> and
    std::same_as<typename result_traits<T>::value_type, acceptance>;

} // detail

// Effects belong to the consumer. Values are valid during the call; retention,
// resume cursors and durable commit are separate contracts. On stop, do not
// increment again: input ranges may fetch their next value during increment.
// An expected error stops before consuming the failing row. Its progress records
// that row as offered and rejected; callback effects are not rolled back.
template <std::ranges::input_range RangeT, typename ConsumerT>
requires std::invocable<ConsumerT&, std::ranges::range_reference_t<RangeT>> and
         detail::acceptance_result<
             std::invoke_result_t<ConsumerT&, std::ranges::range_reference_t<RangeT>>>
[[nodiscard]] constexpr auto visit_range(RangeT&& source, ConsumerT&& consumer)
{
    using decision_type = std::invoke_result_t<ConsumerT&, std::ranges::range_reference_t<RangeT>>;
    using traits = detail::result_traits<decision_type>;
    using error_type = typename traits::error_type;
    using result_type = std::conditional_t<std::is_void_v<error_type>, visit_result,
        std::expected<visit_result, visit_failure<error_type>>>;

    visit_result result;
    auto current = std::ranges::begin(source);

    const auto finish = std::ranges::end(source);
    for (; current != finish; ++current) {
        ++result.offered;
        auto response = std::invoke(consumer, *current);
        if constexpr (!std::is_void_v<error_type>) {
            if (!response) {
                result.stop = acceptance::reject_and_stop;

                return result_type {std::unexpected(
                    visit_failure<error_type> {result, std::move(response.error())})};
            }
        }
        const auto decision = detail::result_value(response);
        if (acceptance::accept == decision || acceptance::accept_and_stop == decision) {
            ++result.accepted;
        }
        if (acceptance::accept_and_stop == decision || acceptance::reject_and_stop == decision) {
            result.stop = decision;

            return result_type {result};
        }
    }

    return result_type {result};
}

namespace detail {

template <typename DestinationT, typename RangeT>
concept member_appendable_range = requires (DestinationT& destination, RangeT&& source) {
    destination.append_range(std::forward<RangeT>(source));
};

template <typename DestinationT, typename RangeT>
concept push_backable_range =
    requires (DestinationT& destination, std::ranges::range_reference_t<RangeT> value) {
        destination.push_back(std::forward<decltype(value)>(value));
    } and
    (!requires { typename DestinationT::value_type; } ||
     std::constructible_from<typename DestinationT::value_type,
                             std::ranges::range_reference_t<RangeT>>);

template <typename T>
inline constexpr bool string_destination = false;

template <typename CharacterT, typename TraitsT, typename AllocatorT>
inline constexpr bool string_destination<std::basic_string<CharacterT, TraitsT, AllocatorT>> = true;

template <typename DestinationT, std::ranges::input_range RangeT>
constexpr void append_elements(DestinationT& destination, RangeT&& source)
{
    if constexpr (std::ranges::sized_range<RangeT> and requires {
                      destination.size();
                      destination.max_size();
                  }) {
        const auto count = std::ranges::size(source);
        const auto current = destination.size();
        if (std::cmp_greater(count, destination.max_size() - current)) {
            throw std::length_error {"appended range exceeds destination capacity"};
        }
    }
    // Preserve the container's amortized growth. Reserving exactly size() + count
    // on every small append would turn repeated insertion into quadratic copying.
    // Observe the actual range reference, including move-only rvalues. Algorithm
    // callable checks can additionally require invocation on value_type lvalues.
    for (auto&& value : std::ranges::ref_view {source}) {
        destination.push_back(std::forward<decltype(value)>(value));
    }
}

} // detail

// Appends elements in source order and returns the destination. References retain their
// value category: use views::as_rvalue explicitly to consume elements of an owning range.
// Source must not overlap the destination, except for basic_string, which retains its
// standard overlap support. Allocation and failure effects follow the destination;
// elementwise insertion can leave an appended prefix if an element operation throws.
template <typename DestinationT, std::ranges::input_range RangeT>
requires detail::member_appendable_range<DestinationT, RangeT> ||
         detail::push_backable_range<DestinationT, RangeT>
constexpr DestinationT& append_range(DestinationT& destination, RangeT&& source)
{
    if constexpr (std::ranges::sized_range<RangeT>) {
        if (std::ranges::empty(source)) {
            return destination;
        }
    }

    if constexpr (detail::member_appendable_range<DestinationT, RangeT>) {
        destination.append_range(std::forward<RangeT>(source));

        return destination;
    }

    if constexpr (!detail::member_appendable_range<DestinationT, RangeT>) {
        constexpr bool iterator_append = std::ranges::common_range<RangeT> and
            requires { destination.append(std::ranges::begin(source), std::ranges::end(source)); };

        if constexpr (iterator_append) {
            destination.append(std::ranges::begin(source), std::ranges::end(source));

            return destination;
        }

        if constexpr (!iterator_append and detail::string_destination<DestinationT>) {
            // A single-pass source can refer to the destination's storage. Complete its
            // observation before changing that storage, using the destination's allocator.
            DestinationT staging {destination.get_allocator()};
            detail::append_elements(staging, std::forward<RangeT>(source));
            destination.append(staging);

            return destination;
        }

        if constexpr (!iterator_append and !detail::string_destination<DestinationT>) {
            detail::append_elements(destination, std::forward<RangeT>(source));
        }
    }

    return destination;
}

} // ceph::salad::datum

#endif // CEPH_RGW_DATUM_RANGE_H
