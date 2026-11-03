// Datum: Optional distance and covered quotas above the order-only algebra.
// Finite half-open coverage has explicit units supplied by a metric profile.
// Please keep alignment, native address arithmetic and storage mapping in bindings.
#ifndef CEPH_RGW_DATUM_INTERVAL_METRIC_H
 #define CEPH_RGW_DATUM_INTERVAL_METRIC_H

#include "datum/extent.h"
#include "datum/interval/query.h"

namespace ceph::salad::datum::interval {

// Byte geometry uses ordinary unsigned positions; no application domain declaration is needed.
using byte_domain = comparison_domain<std::uint64_t>;
using byte_query = query<byte_domain>;

constexpr byte_query bounds(const byte_extent value)
{
    return byte_query::closed_open(value.offset, end_offset(value));
}

// Profiles supply distance and its zero; each consumer requests only its arithmetic.
// Order alone never implies a metric.
template <typename MetricT, typename DomainT>
concept coverage_metric_for =
    ordered_domain<DomainT> and std::move_constructible<typename MetricT::quantity_type> and
    std::assignable_from<typename MetricT::quantity_type&, typename MetricT::quantity_type> and
    requires (const MetricT& metric, const typename DomainT::value_type& point) {
        { metric.zero() } -> std::same_as<typename MetricT::quantity_type>;
        { metric.distance(point, point) } -> std::same_as<typename MetricT::quantity_type>;
    };

template <std::unsigned_integral IntegerT>
requires (!std::same_as<IntegerT, bool>)
struct unsigned_metric final
{
    using quantity_type = IntegerT;

public:
    constexpr IntegerT zero() const noexcept { return 0; }

public:
    constexpr IntegerT add(const IntegerT left, const IntegerT right) const
    {
        if (right > std::numeric_limits<IntegerT>::max() - left) {
            throw std::overflow_error {"coverage quantity overflow"};
        }

        return left + right;
    }

public:
    constexpr IntegerT subtract(const IntegerT left, const IntegerT right) const
    {
        if (right > left) {
            throw std::domain_error {"negative coverage quantity"};
        }

        return left - right;
    }

public:
    constexpr IntegerT distance(const IntegerT first, const IntegerT last) const
    {
        return subtract(last, first);
    }

public:
    constexpr IntegerT advance(const IntegerT point, const IntegerT amount) const
    {
        return add(point, amount);
    }

};

inline constexpr unsigned_metric<std::uint64_t> bytes {};

namespace detail {

template <interval_view IntervalT>
constexpr auto finite_half_open_bounds(const IntervalT& value)
{
    const auto bounds = bounds_of(value);
    if (!interval::is_finite(bounds.lower()) || !interval::is_finite(bounds.upper()) ||
        !bounds.lower().inclusive() || bounds.upper().inclusive()) {
        throw std::domain_error {"coverage metric requires finite half-open intervals"};
    }

    return bounds;
}
} // detail

template <canonical_sequence CoverageT, typename MetricT>
requires coverage_metric_for<MetricT, expression_domain_t<CoverageT>> and
         requires (const MetricT& metric, const typename MetricT::quantity_type& quantity) {
             { metric.add(quantity, quantity) } -> std::same_as<typename MetricT::quantity_type>;
         }
constexpr auto measure(const CoverageT& coverage, const MetricT& metric)
{
    auto total = metric.zero();
    for (const auto& value : std::ranges::ref_view {coverage}) {
        const auto bounds = detail::finite_half_open_bounds(value);
        total = metric.add(total, metric.distance(bounds.lower().key(), bounds.upper().key()));
    }

    return total;
}

// Transfer prepared output into an owning canonical view without renormalizing.
// Constructing points and growing the output vector can still copy/allocate.
// Holes consume no quota. Unsupported inclusion/infinity is rejected explicitly;
// discrete stepping and other endpoint conventions require a different profile.
template <canonical_sequence CoverageT, typename MetricT>
requires coverage_metric_for<MetricT, expression_domain_t<CoverageT>> and
         std::totally_ordered<typename MetricT::quantity_type> and
         requires (const MetricT& metric, const typename expression_domain_t<CoverageT>::value_type& point,
                   const typename MetricT::quantity_type& quantity) {
             { metric.advance(point, quantity) }
                 -> std::same_as<typename expression_domain_t<CoverageT>::value_type>;
             { metric.subtract(quantity, quantity) } -> std::same_as<typename MetricT::quantity_type>;
         }
constexpr auto take(const CoverageT& coverage, typename MetricT::quantity_type remaining, const MetricT& metric)
{
    using domain_type = expression_domain_t<CoverageT>;

    std::vector<query<domain_type>> selected;
    if (remaining < metric.zero()) {
        throw std::domain_error {"negative coverage quota"};
    }
    for (const auto& value : std::ranges::ref_view {coverage}) {
        if (remaining == metric.zero()) {
            break;
        }
        const auto bounds = detail::finite_half_open_bounds(value);
        const auto& first = bounds.lower().key();
        const auto length = metric.distance(first, bounds.upper().key());
        if (remaining < length) {
            selected.push_back(query<domain_type>::closed_open(first, metric.advance(first, remaining)));
            break;
        }

        if constexpr (canonical_interval<decltype(value)>) {
            selected.push_back(value);
        }

        if constexpr (!canonical_interval<decltype(value)>) {
            selected.push_back(query<domain_type>::between(bounds.lower(), bounds.upper()));
        }
        remaining = metric.subtract(remaining, length);
    }

    return detail::canonical_access::adopt(std::move(selected));
}

} // ceph::salad::datum::interval

#endif // CEPH_RGW_DATUM_INTERVAL_METRIC_H
