// Datum: Read-only projection for range libraries without move-only adaptors.
#ifndef CEPH_RGW_DATUM_COMPAT_DETAIL_INTERVAL_PROJECTION_H
 #define CEPH_RGW_DATUM_COMPAT_DETAIL_INTERVAL_PROJECTION_H

#include "datum/compat/projector.h"

#include <compare>
#include <concepts>
#include <type_traits>
#include <utility>

#include <memory>

#include <functional>
#include <iterator>
#include <ranges>

namespace ceph::salad::datum::compat::detail {

template <typename BaseT, typename ResultT, bool = std::ranges::forward_range<BaseT>>
struct projection_iterator_category
{};

template <typename BaseT, typename ResultT>
struct projection_iterator_category<BaseT, ResultT, true>
{
    using base_category = typename std::iterator_traits<
        std::ranges::iterator_t<BaseT>>::iterator_category;
    using iterator_category = std::conditional_t<!std::is_lvalue_reference_v<ResultT>,
        std::input_iterator_tag, std::conditional_t<
            std::derived_from<base_category, std::random_access_iterator_tag>,
            std::random_access_iterator_tag, base_category>>;
};

// Temporary C++23: only the const-invoked projection used by intervals_view.
// The owner stores one source and one callable; iterators borrow both through the view.
template <std::ranges::view ViewT, std::move_constructible ProjectionT>
requires std::ranges::input_range<ViewT> and
         std::regular_invocable<const ProjectionT&, std::ranges::range_reference_t<ViewT>>
class const_projection_view final
    : public std::ranges::view_interface<const_projection_view<ViewT, ProjectionT>>
{
    using storage_type = projector_storage_t<ProjectionT>;

    static constexpr bool trivial_copy_assignment =
        std::is_trivially_copy_assignable_v<ViewT> and
        std::is_trivially_copy_assignable_v<storage_type>;
    static constexpr bool trivial_move_assignment =
        std::is_trivially_move_assignable_v<ViewT> and
        std::is_trivially_move_assignable_v<storage_type>;

    ViewT source;
    [[no_unique_address]] storage_type projection;

    template <bool Const>
    using base_type = std::conditional_t<Const, const ViewT, ViewT>;

    template <bool Const>
    using parent_type = std::conditional_t<Const, const const_projection_view, const_projection_view>;

    static constexpr storage_type default_projection()
    noexcept(std::is_nothrow_default_constructible_v<ProjectionT>)
    requires std::default_initializable<ProjectionT>
    {
        if constexpr (direct_projector_storage<ProjectionT>) {
            return ProjectionT {};
        }

        if constexpr (!direct_projector_storage<ProjectionT>) {
            return storage_type {std::in_place};
        }
    }

    template <bool Const>
    class iterator final : public projection_iterator_category<base_type<Const>,
        std::invoke_result_t<const ProjectionT&, std::ranges::range_reference_t<base_type<Const>>>>
    {
        using base_range = base_type<Const>;
        using base_iterator = std::ranges::iterator_t<base_range>;

        base_iterator current = base_iterator();
        parent_type<Const> *parent = nullptr;

        template <bool> friend class iterator;

    public:
        using iterator_concept = std::conditional_t<std::ranges::random_access_range<base_range>,
            std::random_access_iterator_tag, std::conditional_t<std::ranges::bidirectional_range<base_range>,
                std::bidirectional_iterator_tag, std::conditional_t<std::ranges::forward_range<base_range>,
                    std::forward_iterator_tag, std::input_iterator_tag>>>;
        using value_type = std::remove_cvref_t<
            std::invoke_result_t<const ProjectionT&, std::ranges::range_reference_t<base_range>>>;
        using difference_type = std::ranges::range_difference_t<base_range>;

    public:
        iterator() requires std::default_initializable<base_iterator> = default;

        constexpr iterator(parent_type<Const>& owner, base_iterator position)
            : current(std::move(position)), parent(std::addressof(owner))
        {}

        template <bool Other>
        requires Const and (!Other) and
                 std::convertible_to<std::ranges::iterator_t<base_type<Other>>, base_iterator>
        constexpr iterator(iterator<Other> other)
            : current(std::move(other.current)), parent(other.parent)
        {}

    public:
        constexpr const base_iterator& base() const& noexcept { return current; }

        constexpr base_iterator base() && { return std::move(current); }

        constexpr decltype(auto) operator*() const
        {
            return std::invoke(::ceph::salad::datum::compat::detail::projector_value<ProjectionT>(
                                   parent->projection), *current);
        }

        constexpr iterator& operator++()
        {
            ++current;

            return *this;
        }

        constexpr auto operator++(int)
        {
            if constexpr (std::ranges::forward_range<base_range>) {
                auto previous = *this;
                ++*this;

                return previous;
            }

            if constexpr (!std::ranges::forward_range<base_range>) {
                ++*this;
            }
        }

        constexpr iterator& operator--() requires std::ranges::bidirectional_range<base_range>
        {
            --current;

            return *this;
        }

        constexpr iterator operator--(int) requires std::ranges::bidirectional_range<base_range>
        {
            auto previous = *this;
            --*this;

            return previous;
        }

        constexpr iterator& operator+=(difference_type count)
        requires std::ranges::random_access_range<base_range>
        {
            current += count;

            return *this;
        }

        constexpr iterator& operator-=(difference_type count)
        requires std::ranges::random_access_range<base_range>
        {
            current -= count;

            return *this;
        }

        constexpr decltype(auto) operator[](difference_type count) const
        requires std::ranges::random_access_range<base_range>
        {
            return std::invoke(::ceph::salad::datum::compat::detail::projector_value<ProjectionT>(
                                   parent->projection), current[count]);
        }

        friend constexpr bool operator==(const iterator& lhs, const iterator& rhs)
        requires std::equality_comparable<base_iterator>
        {
            return lhs.current == rhs.current;
        }

        friend constexpr auto operator<=>(const iterator& lhs, const iterator& rhs)
        requires std::ranges::random_access_range<base_range> and std::three_way_comparable<base_iterator>
        {
            return lhs.current <=> rhs.current;
        }

        friend constexpr bool operator<(const iterator& lhs, const iterator& rhs)
        requires std::ranges::random_access_range<base_range>
        {
            return lhs.current < rhs.current;
        }

        friend constexpr bool operator>(const iterator& lhs, const iterator& rhs)
        requires std::ranges::random_access_range<base_range>
        {
            return rhs < lhs;
        }

        friend constexpr bool operator<=(const iterator& lhs, const iterator& rhs)
        requires std::ranges::random_access_range<base_range>
        {
            return !(rhs < lhs);
        }

        friend constexpr bool operator>=(const iterator& lhs, const iterator& rhs)
        requires std::ranges::random_access_range<base_range>
        {
            return !(lhs < rhs);
        }

        friend constexpr iterator operator+(iterator position, difference_type count)
        requires std::ranges::random_access_range<base_range>
        {
            position += count;

            return position;
        }

        friend constexpr iterator operator+(difference_type count, iterator position)
        requires std::ranges::random_access_range<base_range>
        {
            position += count;

            return position;
        }

        friend constexpr iterator operator-(iterator position, difference_type count)
        requires std::ranges::random_access_range<base_range>
        {
            position -= count;

            return position;
        }

        friend constexpr difference_type operator-(const iterator& lhs, const iterator& rhs)
        requires std::sized_sentinel_for<base_iterator, base_iterator>
        {
            return lhs.current - rhs.current;
        }

    };

    template <bool Const>
    class sentinel final
    {
        using base_sentinel = std::ranges::sentinel_t<base_type<Const>>;

        base_sentinel finish = base_sentinel();

        template <bool> friend class sentinel;

    public:
        sentinel() = default;

        constexpr explicit sentinel(base_sentinel value) : finish(std::move(value))
        {}

        template <bool Other>
        requires Const and (!Other) and
                 std::convertible_to<std::ranges::sentinel_t<base_type<Other>>, base_sentinel>
        constexpr sentinel(sentinel<Other> other) : finish(std::move(other.finish))
        {}

    public:
        template <bool Other>
        requires std::sentinel_for<base_sentinel, std::ranges::iterator_t<base_type<Other>>>
        friend constexpr bool operator==(const iterator<Other>& position, const sentinel& limit)
        {
            return position.base() == limit.finish;
        }

        template <bool Other>
        requires std::sized_sentinel_for<base_sentinel, std::ranges::iterator_t<base_type<Other>>>
        friend constexpr auto operator-(const sentinel& limit, const iterator<Other>& position)
        {
            return limit.finish - position.base();
        }

        template <bool Other>
        requires std::sized_sentinel_for<base_sentinel, std::ranges::iterator_t<base_type<Other>>>
        friend constexpr auto operator-(const iterator<Other>& position, const sentinel& limit)
        {
            return position.base() - limit.finish;
        }

    };

public:
    constexpr const_projection_view()
    noexcept(std::is_nothrow_default_constructible_v<ViewT> and
             std::is_nothrow_default_constructible_v<ProjectionT>)
    requires std::default_initializable<ViewT> and std::default_initializable<ProjectionT>
        : source(), projection(default_projection())
    {}

    constexpr const_projection_view(ViewT value, ProjectionT function)
        : source(std::move(value)), projection(std::move(function))
    {}

    const_projection_view(const const_projection_view&) = default;
    const_projection_view(const_projection_view&&) = default;

public:
    // Default trivial assignments; keep self-assignment checks for effectful members.
    const_projection_view& operator=(const const_projection_view&)
    requires trivial_copy_assignment and std::copy_constructible<ProjectionT> = default;
    const_projection_view& operator=(const_projection_view&&)
    requires trivial_move_assignment = default;

    constexpr const_projection_view& operator=(const const_projection_view& other)
    noexcept(std::is_nothrow_copy_assignable_v<ViewT> and
             noexcept(::ceph::salad::datum::compat::detail::assign_projector<ProjectionT>(
                 projection, other.projection)))
    requires (!trivial_copy_assignment) and std::is_copy_assignable_v<ViewT> and
             std::copy_constructible<ProjectionT>
    {
        if (this != std::addressof(other)) {
            source = other.source;
            ::ceph::salad::datum::compat::detail::assign_projector<ProjectionT>(projection, other.projection);
        }

        return *this;
    }

    constexpr const_projection_view& operator=(const_projection_view&& other)
    noexcept(std::is_nothrow_move_assignable_v<ViewT> and
             noexcept(::ceph::salad::datum::compat::detail::assign_projector<ProjectionT>(
                 projection, std::move(other.projection))))
    requires (!trivial_move_assignment)
    {
        if (this != std::addressof(other)) {
            source = std::move(other.source);
            ::ceph::salad::datum::compat::detail::assign_projector<ProjectionT>(
                projection, std::move(other.projection));
        }

        return *this;
    }

public:
    constexpr auto begin() { return iterator<false> {*this, std::ranges::begin(source)}; }

    constexpr auto begin() const
    requires std::ranges::input_range<const ViewT> and
             std::regular_invocable<const ProjectionT&, std::ranges::range_reference_t<const ViewT>>
    {
        return iterator<true> {*this, std::ranges::begin(source)};
    }

    constexpr auto end()
    {
        if constexpr (std::ranges::common_range<ViewT>) {
            return iterator<false> {*this, std::ranges::end(source)};
        }

        if constexpr (!std::ranges::common_range<ViewT>) {
            return sentinel<false> {std::ranges::end(source)};
        }
    }

    constexpr auto end() const
    requires std::ranges::input_range<const ViewT> and
             std::regular_invocable<const ProjectionT&, std::ranges::range_reference_t<const ViewT>>
    {
        if constexpr (std::ranges::common_range<const ViewT>) {
            return iterator<true> {*this, std::ranges::end(source)};
        }

        if constexpr (!std::ranges::common_range<const ViewT>) {
            return sentinel<true> {std::ranges::end(source)};
        }
    }

    constexpr auto size() requires std::ranges::sized_range<ViewT>
    {
        return std::ranges::size(source);
    }

    constexpr auto size() const requires std::ranges::sized_range<const ViewT>
    {
        return std::ranges::size(source);
    }

};

// Older standard views still support copyable projections; use them when their constraints fit.
template <typename ViewT, typename ProjectionT>
requires requires { typename const_projection_view<ViewT, ProjectionT>; }
consteval auto projection_view_type()
{
    if constexpr (requires { typename std::ranges::transform_view<ViewT, ProjectionT>; }) {
        return std::type_identity<std::ranges::transform_view<ViewT, ProjectionT>> {};
    }

    if constexpr (!requires { typename std::ranges::transform_view<ViewT, ProjectionT>; }) {
        return std::type_identity<const_projection_view<ViewT, ProjectionT>> {};
    }
}

} // ceph::salad::datum::compat::detail

#endif // CEPH_RGW_DATUM_COMPAT_DETAIL_INTERVAL_PROJECTION_H
