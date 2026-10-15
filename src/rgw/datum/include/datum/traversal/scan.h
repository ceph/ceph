// Datum: Exact row consumption and an owned continuation chosen by a profile.
// Positions are saved before publication; continuation does not establish snapshot or commit.
// Please keep native seek, errors, transactions and opaque token rules in bindings.
#ifndef CEPH_RGW_DATUM_TRAVERSAL_SCAN_H
 #define CEPH_RGW_DATUM_TRAVERSAL_SCAN_H

#include "datum/interval/range.h"

namespace ceph::salad::datum::traversal {

template <owning_value StateT>
struct scan_result final
{
    visit_result progress;
    std::optional<StateT> resume;
};

template <typename ProjectionT>
struct ordered_positions final
{
    [[no_unique_address]] ProjectionT projection;

public:
    template <typename RowT>
    requires std::regular_invocable<const ProjectionT&, const RowT&>
    constexpr auto position(const RowT& row) const
    {
        return std::invoke(projection, row);
    }

public:
    template <interval::ordered_domain DomainT>

    constexpr auto next(interval::boundary<DomainT> position, const bool consumed) const
    {
        return std::move(position).with_inclusive(!consumed);
    }

};

template <std::ranges::input_range RangeT, typename ConsumerT, typename PositionsT>
requires std::invocable<ConsumerT&, std::ranges::range_reference_t<RangeT>> &&
         std::same_as<std::invoke_result_t<ConsumerT&, std::ranges::range_reference_t<RangeT>>,
                      acceptance> &&
         requires (PositionsT& positions,
                  const std::remove_reference_t<std::ranges::range_reference_t<RangeT>>& row) {
             positions.position(row);
             requires owning_value<decltype(positions.position(row))>;
             {
                 positions.next(positions.position(row), true)
             } -> std::same_as<decltype(positions.position(row))>;
         }
[[nodiscard]] constexpr auto scan_range(RangeT&& source, ConsumerT&& consumer,
                                        PositionsT&& positions)
{
    using state_type = decltype(positions.position(
        std::declval<const std::remove_reference_t<std::ranges::range_reference_t<RangeT>>&>()));

    scan_result<state_type> result;
    result.progress = visit_range(std::forward<RangeT>(source), [&consumer, &positions, &result](auto&& row) {
        // A profile may use a cheap index/token or materialize a native key.
        // Save an owned position before moving rows or producing effects.
        auto saved = positions.position(std::as_const(row));
        const auto decision = std::invoke(consumer, std::forward<decltype(row)>(row));
        if (acceptance::accept_and_stop == decision || acceptance::reject_and_stop == decision) {
            result.resume.emplace(
                positions.next(std::move(saved), acceptance::accept_and_stop == decision));
        }

        return decision;
    });

    return result;
}

} // ceph::salad::datum::traversal

#endif // CEPH_RGW_DATUM_TRAVERSAL_SCAN_H
