// Datum: Source-preserving interval cuts, retention and streaming clipping.
// A data-view layer above the pure order algebra; cuts borrow their current source records.
// Please keep native payload ownership, transaction authority and I/O in bindings.

#ifndef CEPH_RGW_DATUM_INTERVAL_CLIP_H
 #define CEPH_RGW_DATUM_INTERVAL_CLIP_H

#include "datum/compat/projector.h"
#include "datum/compat/compiler.h"
#include "datum/interval/metric.h"

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
        if constexpr (std::same_as<std::remove_cvref_t<std::invoke_result_t<const LensT&, const RowT&>>,
                                   byte_extent>) {
            return bounds(std::invoke(std::as_const(lens), row));
        }

        if constexpr (!std::same_as<std::remove_cvref_t<std::invoke_result_t<const LensT&, const RowT&>>,
                                    byte_extent>) {
            return std::invoke(std::as_const(lens), row);
        }
    }

};

// Preserve const observation; mutable-only views may cache iteration metadata.
template <typename SourceT>
using clip_source_t = std::conditional_t<std::ranges::forward_range<const SourceT>,
                                         const SourceT, SourceT>;

template <typename SourceT, typename LensT>
using clip_geometry_result = std::invoke_result_t<
    const clip_geometry<LensT>&, std::ranges::range_reference_t<clip_source_t<SourceT>>>;

template <interval_view LeftT, interval_view RightT>
requires same_expression_domain<LeftT, RightT>
constexpr auto overlap_upper_order(const LeftT& left, const RightT& right)
{
    if (const auto order = compare_position(left.upper(), right.upper()); order != 0) {
        return order;
    }

    return left.upper().inclusive() <=> right.upper().inclusive();
}

} // detail

// Stable lvalue rows and a canonical selection are required. Bounds borrow the
// iterator's geometry cache and selection; consume them before incrementing.
// Rows and selection stay unchanged during observation. Different origins never coalesce.
template <typename SourceT, canonical_sequence SelectionT, typename LensT>
requires std::move_constructible<LensT> and
         std::ranges::forward_range<detail::clip_source_t<SourceT>> and
         std::is_lvalue_reference_v<
             std::ranges::range_reference_t<detail::clip_source_t<SourceT>>> and
         std::regular_invocable<const detail::clip_geometry<LensT>&,
                                std::ranges::range_reference_t<detail::clip_source_t<SourceT>>> and
         stable_interval<detail::clip_geometry_result<SourceT, LensT>> and
         std::same_as<expression_domain_t<SelectionT>,
                      expression_domain_t<detail::clip_geometry_result<SourceT, LensT>>>
class clip_view final : public std::ranges::view_interface<clip_view<SourceT, SelectionT, LensT>>
{
    const SelectionT *selection;

    detail::clip_source_t<SourceT> *source;

    // Restore transform_view when supported libraries provide C++23 move-only adaptors.
    using projector_type = detail::clip_geometry<LensT>;
    using projection_type = compat::detail::projector_storage_t<projector_type>;
    [[no_unique_address]] projection_type projection;

    bool has_empty_origins = false;

    using geometry_result = detail::clip_geometry_result<SourceT, LensT>;
    using geometry_type = std::remove_cvref_t<geometry_result>;
    // Borrowed geometry needs only a nullable pointer, not an optional flag.
    using geometry_cache = std::conditional_t<std::is_lvalue_reference_v<geometry_result>,
                                                const geometry_type*,
                                                std::optional<geometry_type>>;

public:
    using domain_type = expression_domain_t<SelectionT>;

public:
    constexpr clip_view(SourceT& rows, SelectionT& wanted, LensT lens)
        : selection(std::addressof(wanted)), source(std::addressof(rows)),
          projection(projector_type {std::move(lens)})
    {
        // Identity preserves the canonical source's existing admission proof.
        if constexpr (canonical_sequence<SourceT> and std::same_as<LensT, std::identity>) {
            return;
        }
        // Retain each projection once while validating disjoint record order.
        geometry_cache previous {};
        for (auto row = std::ranges::begin(*source);
             row != std::ranges::end(*source); ++row) {
            decltype(auto) current = std::invoke(projector(), *row);
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
                    (order == 0 and left.upper().inclusive() and right.lower().inclusive())) {
                    throw std::invalid_argument {"clip source is unordered or overlapping"};
                }
            }
            if constexpr (std::is_lvalue_reference_v<geometry_result>) {
                previous = std::addressof(current);
            }

            if constexpr (!std::is_lvalue_reference_v<geometry_result>) {
                previous.emplace(std::forward<geometry_result>(current));
            }
        }
    }

    clip_view(const SourceT&&, SelectionT&, LensT) = delete;
    clip_view(SourceT&, const SelectionT&&, LensT) = delete;
    clip_view(const clip_view&) = default;
    clip_view(clip_view&&) = default;

private:
    constexpr const projector_type& projector() const noexcept
    {
        return compat::detail::projector_value<projector_type>(projection);
    }

    template <typename OtherT>
    constexpr void assign_from(OtherT&& other)
    {
        if (this == std::addressof(other)) {
            return;
        }
        compat::detail::assign_projector<projector_type>(
            projection, std::forward<OtherT>(other).projection);
        selection = other.selection;
        source = other.source;
        has_empty_origins = other.has_empty_origins;
    }

public:
    clip_view& operator=(const clip_view&)
    requires std::is_copy_assignable_v<projection_type> = default;
    clip_view& operator=(clip_view&&)
    requires std::is_move_assignable_v<projection_type> = default;

    constexpr clip_view& operator=(const clip_view& other)
        noexcept(std::is_nothrow_copy_constructible_v<projector_type>)
    requires (!std::is_copy_assignable_v<projection_type>) and
             std::copy_constructible<projector_type>
    {
        assign_from(other);
        return *this;
    }

    constexpr clip_view& operator=(clip_view&& other)
        noexcept(std::is_nothrow_move_constructible_v<projector_type>)
    requires (!std::is_move_assignable_v<projection_type>)
    {
        assign_from(std::move(other));
        return *this;
    }

    class iterator final
    {
        const clip_view *owner = nullptr;

        std::ranges::iterator_t<detail::clip_source_t<SourceT>> row {};
        std::ranges::iterator_t<const SelectionT> wanted {};

        geometry_cache geometry {};

    private:
        constexpr const geometry_type& current_geometry() const { return *geometry; }
        constexpr void load_geometry()
        {
            if constexpr (std::is_lvalue_reference_v<geometry_result>) {
                geometry = std::addressof(std::invoke(owner->projector(), *row));
            }

            if constexpr (!std::is_lvalue_reference_v<geometry_result>) {
                geometry.emplace(std::invoke(owner->projector(), *row));
            }
        }

        constexpr void reset_geometry() noexcept
        {
            if constexpr (std::is_lvalue_reference_v<geometry_result>) {
                geometry = nullptr;
            }

            if constexpr (!std::is_lvalue_reference_v<geometry_result>) {
                geometry.reset();
            }
        }

        DATUM_COMPAT_CXX23_FLATTEN constexpr auto selected_bounds() const
        {
            const auto lhs = detail::bounds_of(current_geometry());
            const auto rhs = detail::bounds_of(*wanted);

            return detail::overlap_bounds(lhs, rhs);
        }

        DATUM_COMPAT_CXX23_FLATTEN constexpr void advance_earlier()
        {
            const auto lhs = detail::bounds_of(current_geometry());
            const auto rhs = detail::bounds_of(*wanted);
            const auto order = detail::overlap_upper_order(lhs, rhs);
            if (order <= 0) {
                ++row;
                reset_geometry();
            }
            if (order >= 0) {
                ++wanted;
            }
        }

        constexpr void seek()
        {
            while (row != std::ranges::end(*owner->source) and
                   wanted != std::ranges::cend(*owner->selection)) {
                if (!geometry) {
                    load_geometry();
                    // Admission established whether empty origins can occur.
                    if (owner->has_empty_origins and is_empty(current_geometry())) {
                        ++row;
                        reset_geometry();
                        continue;
                    }
                }
                const auto lhs = detail::bounds_of(current_geometry());
                const auto rhs = detail::bounds_of(*wanted);
                if (detail::empty_bounds(rhs.lower(), lhs.upper())) {
                    ++row;
                    reset_geometry();
                    continue;
                }
                if (detail::empty_bounds(lhs.lower(), rhs.upper())) {
                    ++wanted;
                    continue;
                }

                return;
            }
            reset_geometry();
        }

    public:
        using iterator_concept = std::input_iterator_tag;
        using difference_type = std::ptrdiff_t;
        using value_type = cut_view<std::ranges::range_value_t<SourceT>, domain_type>;

    public:
        constexpr iterator() = default;
        constexpr explicit iterator(const clip_view& view)
            : owner(std::addressof(view)), row(std::ranges::begin(*view.source)),
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
            reset_geometry();
            if constexpr (std::is_lvalue_reference_v<geometry_result>) {
                geometry = other.geometry;
            }

            if constexpr (!std::is_lvalue_reference_v<geometry_result>) {
                if (other.geometry) {
                    geometry.emplace(std::move(*other.geometry));
                }
            }

            return *this;
        }

        // A cut may borrow this iterator's cache; keep that iterator alive.
        constexpr value_type operator*() const& { return {*row, selected_bounds()}; }
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

namespace detail {

template <typename SourceT, canonical_sequence SelectionT, typename LensT>
struct component_bound<clip_view<SourceT, SelectionT, LensT>>
    : std::integral_constant<std::size_t,
                            add_component_bounds(component_bound_v<SourceT>,
                                                 component_bound_v<SelectionT>)> {};

template <typename RangeT>
using overlap_row_t = std::remove_cvref_t<std::ranges::range_reference_t<RangeT>>;

template <typename RangeT, typename LensT>
using overlap_geometry_t = std::invoke_result_t<const clip_geometry<const LensT&>&,
                                               const overlap_row_t<RangeT>&>;

template <typename RangeT, typename LensT>
concept overlap_source = std::ranges::input_range<RangeT> and
    std::convertible_to<std::ranges::range_reference_t<RangeT>, const overlap_row_t<RangeT>&> and
    std::regular_invocable<const LensT&, const overlap_row_t<RangeT>&> and
    stable_interval<overlap_geometry_t<RangeT, LensT>> and
    (std::is_reference_v<std::ranges::range_reference_t<RangeT>> ||
     std::constructible_from<overlap_row_t<RangeT>, std::ranges::range_reference_t<RangeT>>) and
    (std::is_reference_v<overlap_geometry_t<RangeT, LensT>> ||
     std::move_constructible<std::remove_cvref_t<overlap_geometry_t<RangeT, LensT>>>);

template <typename RangeT, typename LensT>
using overlap_cut_t = cut_view<overlap_row_t<RangeT>,
                               expression_domain_t<overlap_geometry_t<RangeT, LensT>>>;

template <typename RangeT, typename LensT>
requires overlap_source<RangeT, LensT>
class overlap_cursor final
{
    using domain_type = expression_domain_t<overlap_geometry_t<RangeT, LensT>>;

    std::ranges::iterator_t<RangeT> position;
    std::ranges::sentinel_t<RangeT> finish;

    clip_geometry<const LensT&> projection;

    ceph::salad::datum::detail::range_observation<std::ranges::range_reference_t<RangeT>> row;
    ceph::salad::datum::detail::range_observation<overlap_geometry_t<RangeT, LensT>> geometry;

public:
    using cut_type = overlap_cut_t<RangeT, LensT>;

public:
    constexpr overlap_cursor(RangeT& source, const LensT& lens)
        : position(std::ranges::begin(source)),
          finish(std::ranges::end(source)),
          projection {lens}
    {}

public:
    constexpr bool exhausted() const { return position == finish; }

    constexpr void load()
    {
        if (!geometry) {
            row.emplace(*position);
            geometry.emplace(std::invoke(projection, row.get()));
        }
    }

    constexpr void advance()
    {
        geometry.reset();
        row.reset();
        ++position;
    }

    constexpr bool empty() const { return is_empty(geometry.get()); }

    constexpr auto bounds() const { return bounds_of(geometry.get()); }

    constexpr cut_type cut(const borrowed_interval<domain_type> selected) const
    {
        return overlap_cut_t<RangeT, LensT> {row.get(), selected};
    }

};

template <typename RangeT, typename LensT>
concept packed_overlap_source = overlap_source<RangeT, LensT> and
    std::same_as<typename expression_domain_t<overlap_geometry_t<RangeT, LensT>>::value_type,
                 std::uint64_t>;

// The packed start is the exclusive prefix sum of full origin lengths, including skipped origins.
// Adapted from Blelloch, Prefix Sums and Their Applications (1993), sections 1.1 and 1.6:
// https://www.cs.cmu.edu/~guyb/papers/Ble93.pdf
// Keep sequential checked state so geometry errors and stopping retain their traversal order:
template <typename RangeT, typename LensT>
requires packed_overlap_source<RangeT, LensT>
class packed_overlap_cursor final
{
    using domain_type = expression_domain_t<overlap_geometry_t<RangeT, LensT>>;

    overlap_cursor<RangeT, LensT> source;

    std::uint64_t packed_first = 0;
    std::optional<std::uint64_t> packed_last;

public:
    using cut_type = std::pair<typename overlap_cursor<RangeT, LensT>::cut_type, byte_mapping>;

public:
    constexpr packed_overlap_cursor(RangeT& rows, const LensT& lens) : source(rows, lens)
    {}

public:
    constexpr bool exhausted() const { return source.exhausted(); }

    constexpr void load()
    {
        if (packed_last) {
            return;
        }

        source.load();
        packed_last.emplace(source.empty() ? packed_first
            : bytes.add(packed_first, extent_of(source.bounds()).length));
    }

    constexpr void advance()
    {
        packed_first = *packed_last;
        packed_last.reset();
        source.advance();
    }

    constexpr bool empty() const { return source.empty(); }

    constexpr auto bounds() const { return source.bounds(); }

    constexpr cut_type cut(const borrowed_interval<domain_type> selected) const
    {
        const byte_mapping whole {extent_of(source.bounds()), packed_first};

        return {source.cut(selected), ceph::salad::datum::clip(whole, extent_of(selected)).value()};
    }

};

// Ordered co-iteration follows Chou et al., Format Abstraction for Sparse Tensor Algebra
// Compilers (2018), section 4.4: https://compilers.stanford.edu/publications/taco-formats.pdf
// Refinement retains input identities as in Dignös et al., Snapshot Semantics for Temporal
// Multiset Relations (2019), appendix E.2: https://arxiv.org/html/1902.04938
// Here each input is internally disjoint; endpoint inclusion and infinity use Datum's algebra:
template <typename LeftCursorT, typename RightCursorT>
class overlap_stream final
{
    LeftCursorT& left;
    RightCursorT& right;

public:
    using value_type = std::pair<typename LeftCursorT::cut_type, typename RightCursorT::cut_type>;

public:
    constexpr overlap_stream(LeftCursorT& left_source, RightCursorT& right_source)
        : left(left_source),
          right(right_source)
    {}

private:
    constexpr bool exhausted() const { return left.exhausted() || right.exhausted(); }

    constexpr void seek()
    {
        while (!exhausted()) {
            left.load();
            if (left.empty()) {
                left.advance();
                continue;
            }

            right.load();
            if (right.empty()) {
                right.advance();
                continue;
            }

            const auto lhs = left.bounds();
            const auto rhs = right.bounds();
            if (empty_bounds(rhs.lower(), lhs.upper())) {
                left.advance();
                continue;
            }
            if (empty_bounds(lhs.lower(), rhs.upper())) {
                right.advance();
                continue;
            }

            return;
        }
    }

    constexpr value_type selected() const
    {
        const auto lhs = left.bounds();
        const auto rhs = right.bounds();
        const auto shared = overlap_bounds(lhs, rhs);

        return {left.cut(shared), right.cut(shared)};
    }

    constexpr void advance()
    {
        const auto lhs = left.bounds();
        const auto rhs = right.bounds();
        const auto order = overlap_upper_order(lhs, rhs);
        if (order <= 0) {
            left.advance();
        }
        if (order >= 0) {
            right.advance();
        }

        seek();
    }

    class iterator final
    {
        overlap_stream *owner = nullptr;

    public:
        using value_type = overlap_stream::value_type;
        using difference_type = std::ptrdiff_t;
        using iterator_concept = std::input_iterator_tag;

    public:
        constexpr iterator() = default;
        constexpr explicit iterator(overlap_stream& stream) noexcept : owner(std::addressof(stream))
        {}

    public:
        constexpr value_type operator*() const { return owner->selected(); }

        constexpr iterator& operator++()
        {
            owner->advance();

            return *this;
        }

        constexpr void operator++(int) { ++*this; }

        constexpr bool operator==(std::default_sentinel_t) const
        {
            return !owner || owner->exhausted();
        }

    };

public:
    constexpr iterator begin()
    {
        seek();

        return iterator {*this};
    }

    constexpr std::default_sentinel_t end() const noexcept { return {}; }

};

} // detail

// Visit intersecting pieces of ordered, internally disjoint sources without a prepass.
// Each callback receives two cuts with identical bounds and their respective source records.
// Rows, geometry, and cuts are read-only and scoped to the callback; copy needed data before
// retaining it. Value descriptors survive all their cuts, then die before advancing that input.
// Empty rows are skipped; inputs are neither sorted nor validated and need independent traversal
// state. Progress counts cuts, not source rows. Input initialization may itself fetch a row.
// Stop/expected failure does not advance again; exceptions propagate and effects are not undone:
template <std::ranges::input_range LeftT, std::ranges::input_range RightT, typename ConsumerT,
          typename LeftLensT = std::identity, typename RightLensT = std::identity>
requires detail::overlap_source<LeftT, LeftLensT> and
         detail::overlap_source<RightT, RightLensT> and
         same_expression_domain<detail::overlap_geometry_t<LeftT, LeftLensT>,
                                 detail::overlap_geometry_t<RightT, RightLensT>> and
         std::invocable<ConsumerT&, const detail::overlap_cut_t<LeftT, LeftLensT>&,
                                    const detail::overlap_cut_t<RightT, RightLensT>&> and
         ceph::salad::datum::detail::acceptance_result<std::invoke_result_t<
             ConsumerT&, const detail::overlap_cut_t<LeftT, LeftLensT>&,
                         const detail::overlap_cut_t<RightT, RightLensT>&>>
[[nodiscard]] constexpr auto visit_overlaps(LeftT&& left, RightT&& right, ConsumerT&& consumer,
                                          const LeftLensT& left_lens = {},
                                          const RightLensT& right_lens = {})
{
    detail::overlap_cursor<LeftT, LeftLensT> left_cursor {left, left_lens};
    detail::overlap_cursor<RightT, RightLensT> right_cursor {right, right_lens};
    detail::overlap_stream overlaps {left_cursor, right_cursor};

    return ceph::salad::datum::visit_range(overlaps, [&consumer](const auto& cuts) {
        return std::invoke(consumer, cuts.first, cuts.second);
    });
}

// Visit logical overlaps and their positions in a packed source payload. Source coordinates are
// uint64_t bytes; nonempty source geometry and selected cuts must be finite half-open intervals.
// The callback receives source cut, selection cut, and selected byte_mapping. Both origins and
// all borrowed values remain scoped to the callback. Holes contribute no packed bytes, but
// skipped origins contribute their complete lengths. Other contracts match visit_overlaps:
template <std::ranges::input_range SourceT, std::ranges::input_range SelectionT, typename ConsumerT,
          typename SourceLensT = std::identity, typename SelectionLensT = std::identity>
requires detail::packed_overlap_source<SourceT, SourceLensT> and
         detail::overlap_source<SelectionT, SelectionLensT> and
         same_expression_domain<detail::overlap_geometry_t<SourceT, SourceLensT>,
                                 detail::overlap_geometry_t<SelectionT, SelectionLensT>> and
         std::invocable<ConsumerT&, const detail::overlap_cut_t<SourceT, SourceLensT>&,
                                    const detail::overlap_cut_t<SelectionT, SelectionLensT>&,
                                    const byte_mapping&> and
         ceph::salad::datum::detail::acceptance_result<std::invoke_result_t<
             ConsumerT&, const detail::overlap_cut_t<SourceT, SourceLensT>&,
                         const detail::overlap_cut_t<SelectionT, SelectionLensT>&,
                         const byte_mapping&>>
[[nodiscard]] constexpr auto visit_packed_overlaps(SourceT&& source, SelectionT&& selection,
    ConsumerT&& consumer, const SourceLensT& source_lens = {}, const SelectionLensT& selection_lens = {})
{
    detail::packed_overlap_cursor<SourceT, SourceLensT> source_cursor {source, source_lens};
    detail::overlap_cursor<SelectionT, SelectionLensT> selection_cursor {selection, selection_lens};
    detail::overlap_stream overlaps {source_cursor, selection_cursor};

    return ceph::salad::datum::visit_range(overlaps, [&consumer](const auto& cuts) {
        return std::invoke(consumer, cuts.first.first, cuts.second, cuts.first.second);
    });
}

} // ceph::salad::datum::interval

#endif // CEPH_RGW_DATUM_INTERVAL_CLIP_H
