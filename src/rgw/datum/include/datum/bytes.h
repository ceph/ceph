// Datum: Fixed and fragmented byte transfers, cursors, and scoped slices.
// Pure byte operations over standard ranges, independent of any codec.
//
// Please keep native buffers and transport ownership in consumer adapters.
// These spans borrow storage; transferring bytes does not establish admission.

#ifndef CEPH_RGW_DATUM_BYTES_H
 #define CEPH_RGW_DATUM_BYTES_H

#include "datum/detail/byte.h"
#include "datum/extent.h"
#include "datum/range.h"

#include <concepts>
#include <cstddef>
#include <limits>
#include <type_traits>

#include <stdexcept>
#include <utility>

#include <span>

#include <cstring>

#include <algorithm>
#include <functional>
#include <iterator>
#include <ranges>

namespace ceph::salad::datum {

namespace detail {

// Capacity has been admitted by the caller. Representation copying handles
// every byte spelling; constant evaluation and volatile access use value reads.
template <byte_value SourceT, byte_value DestinationT>
constexpr void copy_byte_block(const SourceT *source, DestinationT *destination, const std::size_t count)
{
    if (0 == count) {
        return;
    }
    auto copy_values = [source, destination, count] {
        for (std::size_t index = 0; index < count; ++index) {
            destination[index] = byte_cast<std::remove_cv_t<DestinationT>>(source[index]);
        }
    };
    if consteval {
        copy_values();

        return;
    }

    if constexpr (!std::is_volatile_v<SourceT> and !std::is_volatile_v<DestinationT>) {
        std::memcpy(destination, source, count);

        return;
    }
    copy_values();
}
} // detail

// One contiguous block is one fragment. The view preserves element constness and
// borrows the bytes; a temporary owner cannot supply this escaping view.
template <borrowed_byte_block SourceT>
constexpr auto fragments(SourceT&& source)
{
    using byte_type = std::remove_reference_t<std::ranges::range_reference_t<SourceT>>;

    return std::views::single(
        std::span<byte_type> {std::ranges::data(source), std::ranges::size(source)});
}

// Sum fragment byte counts with checked arithmetic.
template <byte_fragments FragmentsT>
constexpr std::size_t byte_capacity(FragmentsT&& fragments)
{
    std::size_t total = 0;
    for (auto&& fragment : std::ranges::ref_view {detail::read_fragments(fragments)}) {
        const auto size = std::ranges::size(fragment);
        if (size > std::numeric_limits<std::size_t>::max() - total) {
            throw std::length_error {"byte fragment size overflow"};
        }
        total += size;
    }

    return total;
}

// Preflight capacity before the first write. Source and destination must not
// overlap. Fragment descriptors and their backing storage survive this call.
template <byte_block SourceT, writable_byte_fragments FragmentsT>
constexpr std::size_t scatter_bytes(SourceT&& source, FragmentsT&& destination)
{
    if (std::ranges::size(source) > byte_capacity(destination)) {
        throw std::length_error {"byte fragments have insufficient capacity"};
    }
    std::size_t copied = 0;
    static_cast<void>(ceph::salad::datum::visit_range(destination, [&](auto&& fragment) {
        const auto count =
            std::min<std::size_t>(std::ranges::size(source) - copied, std::ranges::size(fragment));
        detail::copy_byte_block(std::ranges::data(source) + copied,
                                std::ranges::data(fragment), count);
        copied += count;
        return copied == std::ranges::size(source) ? acceptance::accept_and_stop
                                                   : acceptance::accept;
    }));

    return copied;
}

// Datum: Bounded, borrowing byte slices and cursors over standard fragments.
// Descriptors retain positions; byte storage stays with the caller's owner.
// Please keep native buffers, I/O, and transport lifetime rules in bindings.

template <std::forward_iterator IteratorT>
requires byte_block<std::iter_reference_t<IteratorT>>
class byte_slice_view final : public std::ranges::view_interface<byte_slice_view<IteratorT>>
{
    IteratorT first {};

    std::size_t offset = 0;
    std::size_t length = 0;

public:
    using byte_type =
        std::remove_reference_t<std::ranges::range_reference_t<std::iter_reference_t<IteratorT>>>;

    class iterator final
    {
        IteratorT fragment {};
        std::size_t offset = 0;
        std::size_t remaining = 0;

    private:
        constexpr void skip_empty()
        {
            while (remaining and offset == std::ranges::size(*fragment)) {
                ++fragment;
                offset = 0;
            }
        }

    public:
        using value_type = std::span<const byte_type>;
        using difference_type = std::ptrdiff_t;
        using iterator_concept = std::forward_iterator_tag;
        using iterator_category = std::input_iterator_tag;

    public:
        constexpr iterator() = default;
        constexpr iterator(IteratorT source, const std::size_t start, const std::size_t count)
            : fragment(source), offset(start), remaining(count)
        {
            skip_empty();
        }

    public:
        constexpr value_type operator*() const
        requires detail::stable_byte_block<std::iter_reference_t<IteratorT>>
        {
            auto&& block = *fragment;

            return std::span<const byte_type> {std::ranges::data(block), std::ranges::size(block)}
                .subspan(offset, std::min<std::size_t>(remaining, std::ranges::size(block) - offset));
        }

        constexpr iterator& operator++()
        {
            remaining -= std::size(**this);
            if (remaining) {
                ++fragment;
                offset = 0;
                skip_empty();
            }

            return *this;
        }

        constexpr iterator operator++(int)
        {
            auto saved = *this;
            ++*this;

            return saved;
        }

        constexpr bool operator==(const iterator&) const = default;
        constexpr bool operator==(std::default_sentinel_t) const { return 0 == remaining; }

    };

public:
    constexpr byte_slice_view() = default;
    constexpr byte_slice_view(IteratorT source, const std::size_t start, const std::size_t count)
        : first(source), offset(start), length(count)
    {}

public:
    constexpr iterator begin() const { return {first, offset, length}; }

    constexpr std::default_sentinel_t end() const { return {}; }

public:
    // Range elements are fragments; byte count is deliberately not range size().
    constexpr std::size_t byte_size() const noexcept { return length; }

};

template <byte_fragments FragmentsT>
class byte_cursor final
{
    using fragments_type = std::remove_const_t<std::remove_reference_t<FragmentsT>>;
    using source_type = detail::fragment_source_t<FragmentsT>;

    std::ranges::iterator_t<source_type> current;
    std::ranges::sentinel_t<source_type> finish;
    std::size_t offset = 0;

    std::size_t available;
    std::size_t capacity;

private:
    constexpr void skip_empty()
    {
        while (current != finish and offset == std::ranges::size(*current)) {
            ++current;
            offset = 0;
        }
    }

    template <typename ConsumerT>
    constexpr void advance(const std::size_t count, ConsumerT&& consume_block)
    {
        if (count > available) {
            throw std::length_error {"byte cursor has insufficient input"};
        }
        auto left = count;
        while (left) {
            auto&& block = *current;
            const auto step = std::min<std::size_t>(left, std::ranges::size(block) - offset);
            std::invoke(consume_block, block, offset, count - left, step);
            offset += step;
            left -= step;
            available -= step;
            skip_empty();
        }
    }

public:
    using slice_type = byte_slice_view<std::ranges::iterator_t<source_type>>;

    class bookmark final
    {
        friend class byte_cursor;

        const byte_cursor *owner;

        std::ranges::iterator_t<source_type> fragment;

        std::size_t offset;
        std::size_t available;

    private:
        constexpr bookmark(const byte_cursor& cursor)
            : owner(&cursor), fragment(cursor.current), offset(cursor.offset),
              available(cursor.available)
        {}

    };

public:
    constexpr explicit byte_cursor(source_type& fragments)

        : current(std::ranges::begin(fragments)), finish(std::ranges::end(fragments)),
          available(byte_capacity(fragments)), capacity(available)
    {
        skip_empty();
    }

    byte_cursor(fragments_type&&) = delete;
    byte_cursor(const fragments_type&&) = delete;
    byte_cursor(const byte_cursor&) = delete;
    byte_cursor(byte_cursor&&) = delete;

public:
    constexpr std::size_t remaining() const noexcept { return available; }

public:
    constexpr std::size_t position() const noexcept { return capacity - available; }

public:
    constexpr bookmark mark() const { return bookmark {*this}; }

public:
    constexpr void restore(const bookmark& saved)
    {
        if (saved.owner != this) {
            throw std::invalid_argument {"byte bookmark belongs to another cursor"};
        }
        current = saved.fragment;
        offset = saved.offset;
        available = saved.available;
    }

public:
    constexpr void consume(const std::size_t count)
    {
        advance(count, [](const auto&, std::size_t, std::size_t, std::size_t) {});
    }

public:
    constexpr slice_type take(const std::size_t count)
    requires detail::stable_byte_block<std::ranges::range_reference_t<source_type>>
    {
        const auto slice = slice_type {current, offset, count};
        consume(count);

        return slice;
    }

public:
    template <writable_byte_output OutputT>
    constexpr void read_into(OutputT&& destination)
    {
        advance(std::ranges::size(destination),
                [&destination](auto&& block, const std::size_t source_offset,
                               const std::size_t output_offset, const std::size_t count) {
                    detail::copy_byte_block(std::ranges::data(block) + source_offset,
                                            std::ranges::data(destination) + output_offset, count);
                });
    }

};

template <typename FragmentsT>
byte_cursor(const FragmentsT&) -> byte_cursor<FragmentsT>;

// Copy a bounded region into caller-owned storage. Validate both capacities before writing.
// Source and destination must not overlap. Temporary owning destinations are rejected.
template <byte_source SourceT, writable_byte_output OutputT>
constexpr auto copy(SourceT&& source, const byte_extent selected, OutputT&& output)
{
    if (selected.length > std::ranges::size(output)) {
        throw std::length_error {"byte destination has insufficient capacity"};
    }
    using byte_type = std::remove_reference_t<std::ranges::range_reference_t<OutputT>>;
    auto destination = std::span<byte_type> {std::ranges::data(output), std::ranges::size(output)}
                           .first(static_cast<std::size_t>(selected.length));
    if constexpr (byte_block<SourceT>) {
        if (!detail::extent_fits(selected.offset, selected.length, std::ranges::size(source))) {
            throw std::out_of_range {"byte extent exceeds its source"};
        }
        if (!destination.empty()) {
            detail::copy_byte_block(std::ranges::data(source) + selected.offset,
                                    destination.data(), destination.size());
        }
    }

    if constexpr (!byte_block<SourceT>) {
        byte_cursor<SourceT> input {detail::read_fragments(source)};
        if (!detail::extent_fits(selected.offset, selected.length, input.remaining())) {
            throw std::out_of_range {"byte extent exceeds its source"};
        }
        input.consume(static_cast<std::size_t>(selected.offset));
        input.read_into(destination);
    }

    return destination;
}

// Copy the complete source after preflighting capacity, returning the written prefix.
// Source and output must not overlap. Fragment owners survive this synchronous call.
template <byte_source SourceT, writable_byte_output OutputT>
constexpr auto copy(SourceT&& source, OutputT&& output)
{
    if constexpr (byte_block<SourceT>) {
        return ceph::salad::datum::copy(std::forward<SourceT>(source),
            byte_extent {0, std::ranges::size(source)}, std::forward<OutputT>(output));
    }

    if constexpr (!byte_block<SourceT>) {
        const auto size = byte_capacity(source);
        if (size > std::ranges::size(output)) {
            throw std::length_error {"byte destination has insufficient capacity"};
        }
        std::size_t copied = 0;
        static_cast<void>(ceph::salad::datum::visit_range(
            detail::read_fragments(source), [&](auto&& fragment) {
                const auto count = std::min<std::size_t>(size - copied, std::ranges::size(fragment));
                detail::copy_byte_block(std::ranges::data(fragment),
                                        std::ranges::data(output) + copied, count);
                copied += count;

                return copied == size ? acceptance::accept_and_stop : acceptance::accept;
            }));

        using byte_type = std::remove_reference_t<std::ranges::range_reference_t<OutputT>>;

        return std::span<byte_type> {std::ranges::data(output), size};
    }
}

template <byte_source SourceT, writable_byte_output OutputT>
constexpr auto copy(SourceT&& source, const std::uint64_t offset, OutputT&& output)
{
    return ceph::salad::datum::copy(std::forward<SourceT>(source),
        byte_extent {offset, std::ranges::size(output)}, std::forward<OutputT>(output));
}

// Synchronous consumption: retention of the borrowed blocks is a binding policy.
template <typename FragmentsT, typename SinkT>
requires detail::fragment_input_range<detail::fragment_input_source_t<FragmentsT>> and
         std::invocable<SinkT&,
             const std::remove_reference_t<std::ranges::range_reference_t<
                 detail::fragment_input_source_t<FragmentsT>>>&>
constexpr void write_fragments(FragmentsT&& fragments, SinkT&& sink)
{
    static_cast<void>(ceph::salad::datum::visit_range(
        static_cast<detail::fragment_input_source_t<FragmentsT>&>(fragments),
        [&sink](const auto& block) {
            std::invoke(sink, block);

            return acceptance::accept;
        }));
}

struct byte_slice final
{
    std::size_t offset;
    std::size_t length;

public:
    template <typename BlockT>
    requires byte_block<const BlockT>
    constexpr auto operator()(const BlockT& block) const
    {
        if (!detail::extent_fits(offset, length, std::ranges::size(block))) {
            throw std::length_error {"byte slice exceeds its owner"};
        }
        using byte_type = std::remove_reference_t<std::ranges::range_reference_t<const BlockT>>;

        return std::span<const byte_type> {std::ranges::data(block), std::ranges::size(block)}
            .subspan(offset, length);
    }

    template <typename BlockT>
    void operator()(const BlockT&&) const = delete;

};

} // ceph::salad::datum

namespace std::ranges {

template <typename IteratorT>
inline constexpr bool enable_borrowed_range<::ceph::salad::datum::byte_slice_view<IteratorT>> =
    true;
} // std::ranges

#endif // CEPH_RGW_DATUM_BYTES_H
