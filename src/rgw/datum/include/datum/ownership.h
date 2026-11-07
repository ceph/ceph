// Datum: Recursive payload ownership and owner-preserving projections.
// A data-layer capability; the pure interval kernel does not depend on it.
//
// Please keep this layer independent of native type recognition and blanket ownership
// assumptions.
// Put domain-specific behavior in consumer profiles or adapters.

// -*- mode:C++; tab-width:8; c-basic-offset:4; indent-tabs-mode:nil -*-
// vim: ts=8 sw=4 sts=4 expandtab ft=cpp

#ifndef CEPH_RGW_DATUM_OWNERSHIP_H
 #define CEPH_RGW_DATUM_OWNERSHIP_H

#include "datum/detail/record.h"

#include <cstddef>
#include <meta>
#include <type_traits>

#include <expected>
#include <optional>
#include <tuple>
#include <utility>
#include <variant>

#include <array>
#include <deque>
#include <inplace_vector>
#include <list>
#include <map>
#include <set>
#include <vector>

#include <memory>

#include <functional>
#include <iterator>

#include <string>

#include <chrono>

namespace ceph::salad::datum {

// An owning value does not borrow its payload from the input that produced it.
// Opaque types require an audited specialization. Public aggregates are checked
// recursively, so adding a borrowed member also changes their ownership contract.
template <typename T>
struct ownership_traits;

template <typename T>
concept owning_value = ownership_traits<std::remove_cv_t<T>>::value;

namespace detail {

template <typename T>
consteval bool aggregate_owns_value()
{
    if constexpr (std::is_aggregate_v<T> && !std::is_union_v<T>) {
        constexpr auto context = std::meta::access_context::unprivileged();
        if (!std::empty(std::meta::bases_of(^^T, context))) {
            return false;
        }

        static constexpr auto members = record_members<T>;
        template for (constexpr auto member : members) {
            using member_type = [:std::meta::type_of(member):];

            if constexpr (!owning_value<member_type>) {
                return false;
            }
        }

        return true;
    }

    return false;
}

} // detail

template <typename T>
struct ownership_traits : std::bool_constant<std::is_arithmetic_v<T> || std::is_enum_v<T> ||
                                             detail::aggregate_owns_value<T>()>
{};

template <>
struct ownership_traits<std::string> : std::true_type
{};

template <typename T>
struct ownership_traits<std::vector<T>> : std::bool_constant<owning_value<T>>
{};

template <typename T>
struct ownership_traits<std::deque<T>> : std::bool_constant<owning_value<T>>
{};
template <typename T>
struct ownership_traits<std::list<T>> : std::bool_constant<owning_value<T>>
{};
template <typename T>
struct ownership_traits<std::set<T>> : std::bool_constant<owning_value<T>>
{};
template <typename KeyT, typename ValueT>
struct ownership_traits<std::map<KeyT, ValueT>>
    : std::bool_constant<owning_value<KeyT> && owning_value<ValueT>>
{};
template <typename FirstT, typename SecondT>
struct ownership_traits<std::pair<FirstT, SecondT>>
    : std::bool_constant<owning_value<FirstT> && owning_value<SecondT>>
{};
template <typename ...T>
struct ownership_traits<std::tuple<T...>> : std::bool_constant<(owning_value<T> && ...)>
{};
template <typename T>
struct ownership_traits<std::unique_ptr<T>> : std::bool_constant<owning_value<T>>
{};
template <typename T>
struct ownership_traits<std::unique_ptr<T[]>> : std::bool_constant<owning_value<T>>
{};

template <typename T, std::size_t Size>
struct ownership_traits<T[Size]> : std::bool_constant<owning_value<T>>
{};

template <typename T, std::size_t Size>
struct ownership_traits<std::array<T, Size>> : std::bool_constant<owning_value<T>>
{};

template <typename T, std::size_t Capacity>
struct ownership_traits<std::inplace_vector<T, Capacity>> : std::bool_constant<owning_value<T>>
{};

template <typename T>
struct ownership_traits<std::optional<T>> : std::bool_constant<owning_value<T>>
{};

template <typename ...T>
struct ownership_traits<std::variant<T...>> : std::bool_constant<(owning_value<T> && ...)>
{};

template <typename T, typename ErrorT>
struct ownership_traits<std::expected<T, ErrorT>>
    : std::bool_constant<owning_value<T> && owning_value<ErrorT>>
{};

template <typename ErrorT>
struct ownership_traits<std::expected<void, ErrorT>> : std::bool_constant<owning_value<ErrorT>>
{};

template <typename RepT, typename PeriodT>
struct ownership_traits<std::chrono::duration<RepT, PeriodT>>
    : std::bool_constant<owning_value<RepT>>
{};

template <typename ClockT, typename DurationT>
struct ownership_traits<std::chrono::time_point<ClockT, DurationT>>
    : std::bool_constant<owning_value<DurationT>>
{};

// Datum: A payload owner and a projection, without cached self-references.
// Views are recomputed after moves; retention is distinct from immutability.
// Please enroll native owners in bindings and keep their resource policy there.

template <owning_value OwnerT, typename LensT>
requires std::regular_invocable<const LensT&, const OwnerT&>
class retained_view final
{
    OwnerT owner;
    [[no_unique_address]] LensT lens;

public:
    constexpr retained_view(OwnerT&& value, LensT projection) noexcept(
        std::is_nothrow_move_constructible_v<OwnerT> && std::is_nothrow_move_constructible_v<LensT>)
        : owner(std::move(value)), lens(std::move(projection))
    {}

public:
    constexpr decltype(auto)
    get() const& noexcept(std::is_nothrow_invocable_v<const LensT&, const OwnerT&>)
    {
        return std::invoke(lens, owner);
    }

    void get() const&& = delete;

public:
    constexpr decltype(auto) operator*() const& noexcept(noexcept(get())) { return get(); }
    void operator*() const&& = delete;

};

template <typename OwnerT, typename LensT>
retained_view(OwnerT&&, LensT) -> retained_view<std::remove_cvref_t<OwnerT>, LensT>;

} // ceph::salad::datum

#endif // CEPH_RGW_DATUM_OWNERSHIP_H
