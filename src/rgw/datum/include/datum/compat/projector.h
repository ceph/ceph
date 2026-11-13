// Datum: Internal callable storage for older range libraries.

#ifndef CEPH_RGW_DATUM_COMPAT_PROJECTOR_H
 #define CEPH_RGW_DATUM_COMPAT_PROJECTOR_H

#include <concepts>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>

namespace ceph::salad::datum::compat::detail {

// Match transform_view's callable assignment and exception behavior on older libraries.
// C++23 move-only adaptor support lets the standard view replace this storage.
template <typename ProjectorT>
inline constexpr bool direct_projector_storage =
    std::copy_constructible<ProjectorT>
        ? std::copyable<ProjectorT> ||
              (std::is_nothrow_move_constructible_v<ProjectorT> and
               std::is_nothrow_copy_constructible_v<ProjectorT>)
        : std::movable<ProjectorT> || std::is_nothrow_move_constructible_v<ProjectorT>;

template <typename ProjectorT>
using projector_storage_t = std::conditional_t<direct_projector_storage<ProjectorT>, ProjectorT,
                                               std::optional<ProjectorT>>;

template <typename ProjectorT>
constexpr const ProjectorT& projector_value(const projector_storage_t<ProjectorT>& storage) noexcept
{
    if constexpr (direct_projector_storage<ProjectorT>) {
        return storage;
    }

    if constexpr (!direct_projector_storage<ProjectorT>) {
        return *storage;
    }
}

// The owning view checks self-assignment before reconstructing its nonassignable callable.
template <typename ProjectorT, typename SourceT>
constexpr void assign_projector(projector_storage_t<ProjectorT>& target, SourceT&& source)
noexcept(std::is_assignable_v<projector_storage_t<ProjectorT>&, SourceT>
             ? std::is_nothrow_assignable_v<projector_storage_t<ProjectorT>&, SourceT>
             : std::is_nothrow_constructible_v<projector_storage_t<ProjectorT>, SourceT>)
{
    constexpr bool assignable = std::is_assignable_v<projector_storage_t<ProjectorT>&, SourceT>;

    if constexpr (assignable) {
        target = std::forward<SourceT>(source);

        return;
    }

    if constexpr (!assignable and direct_projector_storage<ProjectorT>) {
        // Direct reconstruction is selected only when it cannot throw.
        std::destroy_at(std::addressof(target));
        std::construct_at(std::addressof(target), std::forward<SourceT>(source));

        return;
    }

    if constexpr (!assignable and !direct_projector_storage<ProjectorT>) {
        // A throwing reconstruction leaves a valid disengaged owner for reassignment/destruction.
        target.reset();

        if (source) {
            target.emplace(*std::forward<SourceT>(source));
        }
    }
}

} // ceph::salad::datum::compat::detail

#endif // CEPH_RGW_DATUM_COMPAT_PROJECTOR_H
