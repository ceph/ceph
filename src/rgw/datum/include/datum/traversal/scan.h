// Datum: Exact row consumption and an owned continuation chosen by a profile.
// Capture precedes publication; resume does not establish snapshot or commit.
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

template <typename CaptureT>
struct ordered_positions final
{
    [[no_unique_address]] CaptureT projection;

public:
    template <typename RowT>
    requires std::regular_invocable<const CaptureT&, const RowT&>
    constexpr auto capture(const RowT& row) const
    {
        return std::invoke(projection, row);
    }

public:
    template <interval::ordered_domain DomainT>

    constexpr auto resume(interval::boundary<DomainT> position, const bool consumed) const
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
             positions.capture(row);
             requires owning_value<decltype(positions.capture(row))>;
             {
                 positions.resume(positions.capture(row), true)
             } -> std::same_as<decltype(positions.capture(row))>;
         }
[[nodiscard]] constexpr auto scan_range(RangeT&& source, ConsumerT&& consumer,
                                        PositionsT&& positions)
{
    using state_type = decltype(positions.capture(
        std::declval<const std::remove_reference_t<std::ranges::range_reference_t<RangeT>>&>()));

    scan_result<state_type> result;
    result.progress = visit_range(std::forward<RangeT>(source), [&consumer, &positions, &result](auto&& row) {
        // A profile may use a cheap index/token or materialize a native key.
        // Required ownership is captured before moving rows or producing effects.
        auto saved = positions.capture(std::as_const(row));
        const auto decision = std::invoke(consumer, std::forward<decltype(row)>(row));
        if (acceptance::accept_and_stop == decision || acceptance::reject_and_stop == decision) {
            result.resume.emplace(
                positions.resume(std::move(saved), acceptance::accept_and_stop == decision));
        }

        return decision;
    });

    return result;
}

} // ceph::salad::datum::traversal

#endif // CEPH_RGW_DATUM_TRAVERSAL_SCAN_H
