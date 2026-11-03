// Datum: Temporary lvalue-only iteration for frontends without explicit-object parameters.
#ifndef CEPH_RGW_DATUM_COMPAT_DETAIL_INTERVAL_RANGE_H
 #define CEPH_RGW_DATUM_COMPAT_DETAIL_INTERVAL_RANGE_H

#include <ranges>
#include <utility>

namespace ceph::salad::datum::compat::detail {

// Retire this empty facade when supported compilers implement explicit-object parameters.
// intervals_view grants access to its existing projection; no provider hook or storage is added.
template <typename DerivedT>
class interval_range_interface : public std::ranges::view_interface<DerivedT>
{
public:
    constexpr auto begin() &
    requires std::ranges::range<decltype((std::declval<DerivedT&>().projection))>
    {
        return std::ranges::begin(static_cast<DerivedT&>(*this).projection);
    }

    constexpr auto begin() const&
    requires std::ranges::range<decltype((std::declval<const DerivedT&>().projection))>
    {
        return std::ranges::begin(static_cast<const DerivedT&>(*this).projection);
    }

    void begin() const&& = delete;

    constexpr auto end() &
    requires std::ranges::range<decltype((std::declval<DerivedT&>().projection))>
    {
        return std::ranges::end(static_cast<DerivedT&>(*this).projection);
    }

    constexpr auto end() const&
    requires std::ranges::range<decltype((std::declval<const DerivedT&>().projection))>
    {
        return std::ranges::end(static_cast<const DerivedT&>(*this).projection);
    }

    void end() const&& = delete;

    constexpr auto size() &
    requires std::ranges::sized_range<decltype((std::declval<DerivedT&>().projection))>
    {
        return std::ranges::size(static_cast<DerivedT&>(*this).projection);
    }

    constexpr auto size() const&
    requires std::ranges::sized_range<decltype((std::declval<const DerivedT&>().projection))>
    {
        return std::ranges::size(static_cast<const DerivedT&>(*this).projection);
    }

    void size() && = delete;

};

} // ceph::salad::datum::compat::detail

#endif // CEPH_RGW_DATUM_COMPAT_DETAIL_INTERVAL_RANGE_H
