// Standard-backed projection and lexicographic comparison composition.
#ifndef CEPH_RGW_DATUM_COMPARE_H
 #define CEPH_RGW_DATUM_COMPARE_H

#include <compare>
#include <concepts>
#include <cstddef>
#include <type_traits>

#include <tuple>
#include <utility>

#include <functional>

namespace ceph::salad::datum {

namespace detail {

template <typename T>
concept ordering_result = std::same_as<std::remove_cvref_t<T>, std::strong_ordering> ||
                          std::same_as<std::remove_cvref_t<T>, std::weak_ordering> ||
                          std::same_as<std::remove_cvref_t<T>, std::partial_ordering>;

template <typename ComparisonT, typename LeftT, typename RightT>
concept ordering_for =
    std::invocable<ComparisonT, const LeftT&, const RightT&> and
    ordering_result<std::invoke_result_t<ComparisonT, const LeftT&, const RightT&>>;

template <typename T>
concept sorting_ordering_result = ordering_result<T> and
    (!std::same_as<std::remove_cvref_t<T>, std::partial_ordering>);

template <typename ComparisonT, typename LeftT, typename RightT>
concept sorting_comparison_for =
    std::invocable<ComparisonT, const LeftT&, const RightT&> and
    sorting_ordering_result<std::invoke_result_t<ComparisonT, const LeftT&, const RightT&>>;

template <typename LeftT, typename RightT, typename ...ComparisonT>
using comparison_category = std::common_comparison_category_t<
    std::remove_cvref_t<std::invoke_result_t<ComparisonT&, const LeftT&, const RightT&>>...>;

// One value wrapper preserves the callable's actual const and mutable capabilities.
template <typename ImplementationT, typename ...StateT>
struct comparison final
{
    std::tuple<StateT...> state;

private:
    template <typename SelfT, typename LeftT, typename RightT>
    static constexpr decltype(auto) apply(SelfT& self, const LeftT& left, const RightT& right)
    {
        return std::apply([&left, &right](auto& ...parts) -> decltype(auto) {
            return ImplementationT {}(left, right, parts...);
        }, self.state);
    }

public:
    template <typename LeftT, typename RightT>
    requires std::invocable<ImplementationT, const LeftT&, const RightT&, StateT&...>
    constexpr decltype(auto) operator()(const LeftT& left, const RightT& right)
    {
        return apply(*this, left, right);
    }

    template <typename LeftT, typename RightT>
    requires std::invocable<ImplementationT, const LeftT&, const RightT&, const StateT&...>
    constexpr decltype(auto) operator()(const LeftT& left, const RightT& right) const
    {
        return apply(*this, left, right);
    }

};

struct projected_compare final
{
    template <typename LeftT, typename RightT, typename ProjectionT, typename ComparisonT>
    requires std::invocable<ProjectionT&, const LeftT&> and
             std::invocable<ProjectionT&, const RightT&> and
             std::invocable<
                 ComparisonT&,
                 std::add_lvalue_reference_t<std::invoke_result_t<ProjectionT&, const LeftT&>>,
                 std::add_lvalue_reference_t<std::invoke_result_t<ProjectionT&, const RightT&>>>
    constexpr auto operator()(const LeftT& left, const RightT& right, ProjectionT& projection,
                              ComparisonT& compare) const
    {
        auto&& left_key = std::invoke(projection, left);
        auto&& right_key = std::invoke(projection, right);

        return std::invoke(compare, left_key, right_key);
    }
};

struct reversed_compare final
{
    template <typename LeftT, typename RightT, typename ComparisonT>
    requires std::invocable<ComparisonT&, const RightT&, const LeftT&>
    constexpr auto operator()(const LeftT& left, const RightT& right, ComparisonT& compare) const
    {
        return std::invoke(compare, right, left);
    }
};

struct lexicographic_compare final
{
    template <typename LeftT, typename RightT, typename ...ComparisonT>
    requires(ordering_for<ComparisonT&, LeftT, RightT> and ...)
    constexpr auto operator()(const LeftT& left, const RightT& right, ComparisonT& ...compare) const
    {
        using result_type = comparison_category<LeftT, RightT, ComparisonT...>;

        result_type result = std::strong_ordering::equal;
        ((result == 0 ? result = std::invoke(compare, left, right) : result), ...);

        return result;
    }
};

// A sorting predicate needs only the first decision, not a common ordering value.
struct lexicographic_less_compare final
{
    template <typename LeftT, typename RightT>
    static constexpr bool apply(const LeftT&, const RightT&)
    {
        return false;
    }

    template <typename LeftT, typename RightT, typename FirstT, typename ...RestT>
    static constexpr bool apply(const LeftT& left, const RightT& right, FirstT& first, RestT& ...rest)
    {
        if constexpr (0 == sizeof...(RestT)) {
            return std::is_lt(std::invoke(first, left, right));
        }

        if constexpr (0 != sizeof...(RestT)) {
            const auto result = std::invoke(first, left, right);
            if (result != 0) {
                return std::is_lt(result);
            }

            return apply(left, right, rest...);
        }
    }

    template <typename LeftT, typename RightT, typename ...ComparisonT>
    requires(sorting_comparison_for<ComparisonT&, LeftT, RightT> and ...)
    constexpr bool operator()(const LeftT& left, const RightT& right, ComparisonT& ...compare) const
    {
        return apply(left, right, compare...);
    }
};

template <std::size_t Index>
struct nth_key_projection final
{
    template <typename TupleT>
    constexpr auto operator()(TupleT&& value) const
        noexcept(noexcept(std::get<Index>(std::forward<TupleT>(value))))
            -> decltype(std::get<Index>(std::forward<TupleT>(value)))
    {
        return std::get<Index>(std::forward<TupleT>(value));
    }
};

} // namespace detail

template <typename ProjectionT, typename ComparisonT = std::compare_three_way>
constexpr auto ascending(ProjectionT projection, ComparisonT compare = {})
{
    return detail::comparison<detail::projected_compare, ProjectionT, ComparisonT> {
        {std::move(projection), std::move(compare)}};
}

template <typename ComparisonT>
constexpr auto reverse(ComparisonT compare)
{
    return detail::comparison<detail::reversed_compare, ComparisonT> {{std::move(compare)}};
}

template <typename ...ComparisonT>
constexpr auto lexicographic(ComparisonT ...compare)
{
    return detail::comparison<detail::lexicographic_compare, ComparisonT...> {
        {std::move(compare)...}};
}

template <typename ...ComparisonT>
constexpr auto order_by(detail::comparison<detail::lexicographic_compare, ComparisonT...> compare)
{
    return detail::comparison<detail::lexicographic_less_compare, ComparisonT...> {
        std::move(compare.state)};
}

template <typename ProjectionT, typename ComparisonT = std::compare_three_way>
constexpr auto descending(ProjectionT projection, ComparisonT compare = {})
{
    return ceph::salad::datum::reverse(
        ceph::salad::datum::ascending(std::move(projection), std::move(compare)));
}

template <typename ...ComparisonT>
constexpr auto order_by(ComparisonT ...compare)
{
    return detail::comparison<detail::lexicographic_less_compare, ComparisonT...> {
        {std::move(compare)...}};
}

template <std::size_t Index>
inline constexpr detail::nth_key_projection<Index> nth_key {};

} // namespace ceph::salad::datum

#endif // CEPH_RGW_DATUM_COMPARE_H
