// Datum: Source-preserving interval cuts, retention and streaming clipping.
// A data-view layer above the pure order algebra; source records remain borrowed.
// Please keep native payload ownership, transaction authority and I/O in bindings.

#ifndef CEPH_RGW_DATUM_INTERVAL_CLIP_H
 #define CEPH_RGW_DATUM_INTERVAL_CLIP_H

#include "datum/interval/query.h"

namespace ceph::salad::datum::interval {

template <typename RowT, ordered_domain DomainT>
class cut;

// Copying this descriptor preserves its borrow. Bounds supplied by clipping
// can refer to the iterator's geometry cache: consume them before advancement.
template <typename RowT, ordered_domain DomainT>
class cut_view final
{
    const RowT *origin;

    borrowed_interval<DomainT> selected;

public:
    using domain_type = DomainT;

public:
    constexpr cut_view(const RowT& row, const borrowed_interval<DomainT> bounds) noexcept
        : origin(std::addressof(row)), selected(bounds)
    {}
    cut_view(const RowT&&, borrowed_interval<DomainT>) = delete;
    constexpr explicit cut_view(const cut<RowT, DomainT>& retained) noexcept;
    cut_view(const cut<RowT, DomainT>&&) = delete;

public:
    constexpr const RowT& source() const noexcept { return *origin; }
    constexpr const auto& lower() const& noexcept { return selected.lower(); }
    constexpr const auto& upper() const& noexcept { return selected.upper(); }
    const boundary_ref<DomainT>& lower() const&& = delete;
    const boundary_ref<DomainT>& upper() const&& = delete;
    // Legacy scoped geometry access; prefer direct lower()/upper() observation.
    constexpr const auto& bounds() const noexcept { return selected; }

};

// Retain endpoint values; source() still borrows the original record. A point
// type such as string_view retains its ordinary backing-storage lifetime.
template <typename RowT, ordered_domain DomainT>
class cut final
{
    const RowT *origin;

    boundary<DomainT> first;
    boundary<DomainT> last;

public:
    using domain_type = DomainT;

public:
    constexpr explicit cut(const cut_view<RowT, DomainT>& view)
        : origin(std::addressof(view.source())), first(view.lower(), view.lower().inclusive()),
          last(view.upper(), view.upper().inclusive())
    {}

public:
    constexpr const RowT& source() const noexcept { return *origin; }
    constexpr const auto& lower() const& noexcept { return first; }
    constexpr const auto& upper() const& noexcept { return last; }
    const boundary<DomainT>& lower() const&& = delete;
    const boundary<DomainT>& upper() const&& = delete;

};

template <typename RowT, ordered_domain DomainT>
constexpr cut_view<RowT, DomainT>::cut_view(const cut<RowT, DomainT>& retained) noexcept
    : origin(std::addressof(retained.source())),
      selected {detail::borrow_bound(retained.lower()), detail::borrow_bound(retained.upper())}
{}

template <typename RowT, ordered_domain DomainT>
cut_view(const RowT&, borrowed_interval<DomainT>) -> cut_view<RowT, DomainT>;
template <typename RowT, ordered_domain DomainT>
cut_view(const cut<RowT, DomainT>&) -> cut_view<RowT, DomainT>;
template <typename RowT, ordered_domain DomainT>
cut(const cut_view<RowT, DomainT>&) -> cut<RowT, DomainT>;

// Source compatibility for the original borrowed descriptor.
template <typename RowT, ordered_domain DomainT>
using interval_cut = cut_view<RowT, DomainT>;

namespace views {

// Standard transform supplies the closure, iterator, and range protocols.
inline constexpr auto cut = std::views::transform(
    [](auto&& value) noexcept(noexcept(interval::cut {std::forward<decltype(value)>(value)}))
    requires requires { interval::cut {std::forward<decltype(value)>(value)}; }
    { return interval::cut {std::forward<decltype(value)>(value)}; });
} // views

// Datum: Streaming interval cuts that retain their original source association.
// A read-only data view above the order algebra; no payload is copied or merged.
// Please keep storage slicing, alignment, ownership and device rules in bindings.

namespace detail {

template <typename LensT>
struct clip_geometry final
{
    [[no_unique_address]] LensT lens;
    template <typename RowT>
    requires std::regular_invocable<const LensT&, const RowT&>
    constexpr decltype(auto) operator()(const RowT& row) const
    {
        return std::invoke(std::as_const(lens), row);
    }

};

template <typename SourceT, typename LensT>
using clip_geometry_result = std::invoke_result_t<const clip_geometry<LensT>&,
                                                  std::ranges::range_reference_t<const SourceT>>;
} // detail

// Stable lvalue rows and a canonical selection are required. Bounds borrow the
// iterator's geometry cache and selection; consume them before incrementing.
// Neither source may change during observation. Different origins never coalesce.
template <typename SourceT, canonical_sequence SelectionT, typename LensT>
requires std::move_constructible<LensT> && std::ranges::forward_range<const SourceT> &&
         std::is_lvalue_reference_v<std::ranges::range_reference_t<const SourceT>> &&
         std::regular_invocable<const detail::clip_geometry<LensT>&,
                                std::ranges::range_reference_t<const SourceT>> &&
         stable_interval<detail::clip_geometry_result<SourceT, LensT>> &&
         std::same_as<expression_domain_t<SelectionT>,
                      expression_domain_t<detail::clip_geometry_result<SourceT, LensT>>>
class clip_view final : public std::ranges::view_interface<clip_view<SourceT, SelectionT, LensT>>
{
    const SelectionT *selection;

    bool has_empty_origins = false;

    using projection_type = std::ranges::transform_view<std::ranges::ref_view<const SourceT>,
                                                        detail::clip_geometry<LensT>>;

    projection_type projection;

    using geometry_result = detail::clip_geometry_result<SourceT, LensT>;
    using geometry_type = std::remove_cvref_t<geometry_result>;
    using geometry_storage =
        std::conditional_t<std::is_lvalue_reference_v<geometry_result>,
                           std::reference_wrapper<const geometry_type>, geometry_type>;

public:
    using domain_type = expression_domain_t<SelectionT>;

public:
    constexpr clip_view(SourceT& rows, SelectionT& wanted, LensT lens)
        : selection(std::addressof(wanted)),
          projection(std::ranges::ref_view {std::as_const(rows)},
                     detail::clip_geometry<LensT> {std::move(lens)})
    {
        // Identity preserves the canonical source's existing admission proof.
        if constexpr (canonical_sequence<SourceT> && std::same_as<LensT, std::identity>) {
            return;
        }
        // Retain each projection once while validating disjoint record order.
        std::optional<geometry_storage> previous;
        for (auto row = std::ranges::begin(std::as_const(projection));

             row != std::ranges::end(std::as_const(projection)); ++row) {
            decltype(auto) current = *row;
            if (is_empty(current)) {
                has_empty_origins = true;
                continue;
            }
            if (previous) {
                const geometry_type& prior = *previous;
                const auto left = detail::bounds_of(prior);
                const auto right = detail::bounds_of(current);
                const auto order = detail::compare_position(left.upper(), right.lower());
                if (order > 0 ||
                    (order == 0 && left.upper().inclusive() && right.lower().inclusive())) {
                    throw std::invalid_argument {"clip source is unordered or overlapping"};
                }
            }
            previous.emplace(std::forward<geometry_result>(current));
        }
    }

    clip_view(const SourceT&&, SelectionT&, LensT) = delete;
    clip_view(SourceT&, const SelectionT&&, LensT) = delete;

    class iterator final
    {
        const clip_view *owner = nullptr;

        std::ranges::iterator_t<const projection_type> row {};
        std::ranges::iterator_t<const SelectionT> wanted {};

        std::optional<geometry_storage> geometry;

    private:
        constexpr const geometry_type& current_geometry() const { return *geometry; }
        constexpr void load_geometry() { geometry.emplace(*row); }
        constexpr auto selected_bounds() const
        {
            const auto lhs = detail::bounds_of(current_geometry());
            const auto rhs = detail::bounds_of(*wanted);

            return borrowed_interval<domain_type> {
                        detail::max_bound(lhs.lower(), rhs.lower(), detail::inclusivity_merge::all),
                        detail::min_bound(lhs.upper(), rhs.upper(), detail::inclusivity_merge::all)
                    };
        }

        constexpr void advance_earlier()
        {
            const auto lhs = detail::bounds_of(current_geometry());
            const auto rhs = detail::bounds_of(*wanted);
            auto order = detail::compare_position(lhs.upper(), rhs.upper());
            if (order == 0) {
                order = lhs.upper().inclusive() <=> rhs.upper().inclusive();
            }
            if (order <= 0) {
                ++row;
                geometry.reset();
            }
            if (order >= 0) {
                ++wanted;
            }
        }

        constexpr void seek()
        {
            while (row != std::ranges::end(owner->projection) &&
                   wanted != std::ranges::cend(*owner->selection)) {
                if (!geometry) {
                    load_geometry();
                    // Admission established whether empty origins can occur.
                    if (owner->has_empty_origins && is_empty(current_geometry())) {
                        ++row;
                        geometry.reset();
                        continue;
                    }
                }
                const auto lhs = detail::bounds_of(current_geometry());
                const auto rhs = detail::bounds_of(*wanted);
                if (detail::empty_bounds(rhs.lower(), lhs.upper())) {
                    ++row;
                    geometry.reset();
                    continue;
                }
                if (detail::empty_bounds(lhs.lower(), rhs.upper())) {
                    ++wanted;
                    continue;
                }

                return;
            }
            geometry.reset();
        }

    public:
        using iterator_concept = std::input_iterator_tag;
        using difference_type = std::ptrdiff_t;
        using value_type = cut_view<std::ranges::range_value_t<SourceT>, domain_type>;

    public:
        constexpr iterator() = default;
        constexpr explicit iterator(const clip_view& view)
            : owner(std::addressof(view)), row(std::ranges::begin(view.projection)),
              wanted(std::ranges::cbegin(*view.selection))
        {
            seek();
        }

        constexpr iterator(const iterator&) = default;
        constexpr iterator(iterator&&) = default;

    public:
        // Geometry need only be constructible; strong points need not be assignable.
        constexpr iterator& operator=(iterator other)
        {
            owner = other.owner;
            row = std::move(other.row);
            wanted = std::move(other.wanted);
            geometry.reset();
            if (other.geometry) {
                geometry.emplace(std::move(*other.geometry));
            }

            return *this;
        }

        // A cut may borrow this iterator's cache; keep that iterator alive.
        constexpr value_type operator*() const& { return {*row.base(), selected_bounds()}; }
        value_type operator*() const&& = delete;

        constexpr iterator& operator++()
        {
            advance_earlier();
            seek();

            return *this;
        }

        constexpr void operator++(int) { ++*this; }

        // Between operations, retained geometry means a nonempty cut is ready.
        constexpr bool operator==(std::default_sentinel_t) const noexcept { return !geometry; }

    };

public:
    // Iteration observes the view's stored lens and source metadata.
    constexpr iterator begin() const& { return iterator {*this}; }
    iterator begin() const&& = delete;

    constexpr std::default_sentinel_t end() const noexcept { return {}; }

};

template <typename SourceT, typename SelectionT, typename LensT>
clip_view(SourceT&, SelectionT&, LensT) -> clip_view<SourceT, SelectionT, LensT>;

} // ceph::salad::datum::interval

#endif // CEPH_RGW_DATUM_INTERVAL_CLIP_H
